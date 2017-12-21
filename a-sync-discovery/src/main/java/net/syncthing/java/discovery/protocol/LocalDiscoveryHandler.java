/* 
 * Copyright (C) 2016 Davide Imbriaco
 *
 * This Java file is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syncthing.java.discovery.protocol;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import net.syncthing.java.core.beans.DeviceAddress;
import net.syncthing.java.core.configuration.ConfigurationService;
import net.syncthing.java.core.events.DeviceAddressReceivedEvent;
import net.syncthing.java.core.security.KeystoreHandler;
import net.syncthing.java.discovery.protocol.LocalDiscoveryProtos.Announce;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Objects.equal;
import static com.google.common.base.Preconditions.checkArgument;

public final class LocalDiscoveryHandler implements Closeable {

    private final static int MAGIC = 0x2EA7D90B;
    private final static int LISTENING_PORT = 21027;
    private final static int MAX_WAIT = 60 * 1000;
    private final int INCOMING_BUFFER_SIZE = 1024;

    private final Logger mLogger = LoggerFactory.getLogger(getClass());
    private final ScheduledExecutorService mListeningExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService mProcessingExecutor = Executors.newCachedThreadPool();
    private final EventBus mEventBus = new AsyncEventBus(mProcessingExecutor);
    private final Multimap<String, DeviceAddress> mLocalDiscoveryRecords = HashMultimap.create();
    private final ConfigurationService mConfiguration;
    private final int mListenPort;

    private DatagramSocket mDatagramSocket;

    public LocalDiscoveryHandler(ConfigurationService configuration, int listenPort) {
        mConfiguration = configuration;
        mListenPort = listenPort;
    }

    public Collection<DeviceAddress> queryAndClose(String deviceId) {
        final Object lock = new Object();
        mEventBus.register(new Object() {
            @Subscribe
            public void handleMessageReceivedEvent(MessageReceivedEvent event) {
                synchronized (lock) {
                    if (deviceId.equals(event.getDeviceId())) {
                        lock.notify();
                    }
                }
            }
        });
        startListener();
        sendAnnounceMessage();
        synchronized (lock) {
            try {
                lock.wait(MAX_WAIT);
            } catch (InterruptedException ex) {
                mLogger.warn("", ex);
            }
        }
        close();
        synchronized (mLocalDiscoveryRecords) {
            return mLocalDiscoveryRecords.get(deviceId);
        }
    }

    public void sendAnnounceMessage() {
        mProcessingExecutor.submit(() -> {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                new DataOutputStream(out).writeInt(MAGIC);
                mLogger.info("sending out discovery message with port {}", mListenPort);
                Announce.newBuilder()
                        .setId(ByteString.copyFrom(KeystoreHandler.deviceIdStringToHashData(mConfiguration.getDeviceId())))
                        .addAddresses("tcp://:" + mListenPort)
                        .setInstanceId(mConfiguration.getInstanceId())
                        .build().writeTo(out);
                byte[] data = out.toByteArray();
                Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                while (networkInterfaces.hasMoreElements()) {
                    NetworkInterface networkInterface = networkInterfaces.nextElement();
                    for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                        InetAddress broadcastAddress = interfaceAddress.getBroadcast();
                        mLogger.trace("interface = {} address = {} broadcast = {}", networkInterface, interfaceAddress, broadcastAddress);
                        if (broadcastAddress != null) {
                            mLogger.debug("sending broadcast announce on {}", broadcastAddress);
                            try (DatagramSocket broadcastSocket = new DatagramSocket()) {
                                broadcastSocket.setBroadcast(true);
                                DatagramPacket datagramPacket = new DatagramPacket(
                                        data, data.length, broadcastAddress, LISTENING_PORT);
                                broadcastSocket.send(datagramPacket);
                            }
                        }
                    }
                }
            } catch (IOException e) {
                mLogger.warn("Failed to send local announce message", e);
            }
        });
    }

    public void startListener() {
        if (mDatagramSocket == null || mDatagramSocket.isClosed()) {
            try {
                mDatagramSocket = new DatagramSocket(LISTENING_PORT, InetAddress.getByName("0.0.0.0"));
                mLogger.info("Opened udp socket {}", mDatagramSocket.getLocalSocketAddress());
            } catch (IOException e) {
                mLogger.warn("Failed to open listening socket on port {}", LISTENING_PORT, e);
                return;
            }
        }

        mListeningExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    final DatagramPacket datagramPacket =
                            new DatagramPacket(new byte[INCOMING_BUFFER_SIZE], INCOMING_BUFFER_SIZE);
                    mLogger.trace("waiting for message on socket addr = {}",
                            mDatagramSocket.getLocalSocketAddress());
                    mDatagramSocket.receive(datagramPacket);
                    mProcessingExecutor.submit(() -> handleReceivedDatagram(datagramPacket));
                    mListeningExecutor.submit(this);
                } catch (IOException e) {
                    mLogger.warn("Error receiving datagram", e);
                    close();
                }
            }
        });
    }

    private void handleReceivedDatagram(DatagramPacket datagramPacket) {
        try {
            final String sourceAddress = datagramPacket.getAddress().getHostAddress();
            ByteBuffer byteBuffer = ByteBuffer.wrap(
                    datagramPacket.getData(), datagramPacket.getOffset(), datagramPacket.getLength());
            int magic = byteBuffer.getInt();
            checkArgument(magic == MAGIC, "magic mismatch, expected %s, got %s", MAGIC, magic);
            final Announce announce = Announce.parseFrom(ByteString.copyFrom(byteBuffer));
            final String deviceId = KeystoreHandler.hashDataToDeviceIdString(announce.getId().toByteArray());

            // Ignore announcement received from ourselves.
            if (equal(deviceId, mConfiguration.getDeviceId()))
                return;

            mLogger.debug("received local announce from device id = {}", deviceId);
            List<String> addressesList = firstNonNull(announce.getAddressesList(), Collections.emptyList());
            final List<DeviceAddress> deviceAddresses =
                    Lists.newArrayList(Iterables.transform(addressesList, address -> {
                // When interpreting addresses with an unspecified address, e.g.,
                // tcp://0.0.0.0:22000 or tcp://:42424, the source address of the
                // discovery announcement is to be used.
                return DeviceAddress.newBuilder()
                    .setAddress(address.replaceFirst("tcp://(0.0.0.0|):", "tcp://" + sourceAddress + ":"))
                    .setDeviceId(deviceId)
                    .setInstanceId(announce.getInstanceId())
                    .setProducer(DeviceAddress.AddressProducer.LOCAL_DISCOVERY)
                    .build();
            }));
            boolean isNew;
            synchronized (mLocalDiscoveryRecords) {
                isNew = !mLocalDiscoveryRecords.removeAll(deviceId).isEmpty();
                mLocalDiscoveryRecords.putAll(deviceId, deviceAddresses);
            }
            mEventBus.post(new MessageReceivedEvent() {
                @Override
                public List<DeviceAddress> getDeviceAddresses() {
                    return Collections.unmodifiableList(deviceAddresses);
                }

                @Override
                public String getDeviceId() {
                    return deviceId;
                }
            });
            if (isNew) {
                mEventBus.post(new NewLocalPeerEvent() {
                    @Override
                    public String getDeviceId() {
                        return deviceId;
                    }
                });
            }
        } catch (InvalidProtocolBufferException ex) {
            mLogger.warn("error processing datagram", ex);
        }
    }

    public EventBus getEventBus() {
        return mEventBus;
    }

    @Override
    public void close() {
        mProcessingExecutor.shutdown();
        mListeningExecutor.shutdown();
        if (mDatagramSocket != null) {
            IOUtils.closeQuietly(mDatagramSocket);
        }
    }

    public abstract class MessageReceivedEvent implements DeviceAddressReceivedEvent {

        @Override
        public abstract List<DeviceAddress> getDeviceAddresses();

        public abstract String getDeviceId();
    }

    public abstract class NewLocalPeerEvent {

        public abstract String getDeviceId();
    }

}
