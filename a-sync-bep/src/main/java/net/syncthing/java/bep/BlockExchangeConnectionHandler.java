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
package net.syncthing.java.bep;

import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import net.syncthing.java.client.protocol.rp.RelayClient;
import net.syncthing.java.core.beans.DeviceAddress;
import net.syncthing.java.core.beans.DeviceInfo;
import net.syncthing.java.core.beans.FolderInfo;
import net.syncthing.java.core.beans.IndexInfo;
import net.syncthing.java.core.configuration.ConfigurationService;
import net.syncthing.java.core.events.DeviceAddressActiveEvent;
import net.syncthing.java.core.security.KeystoreHandler;
import net.syncthing.java.httprelay.HttpRelayClient;
import net.jpountz.lz4.LZ4Factory;
import net.syncthing.java.bep.BlockExchangeProtos.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocket;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Objects.equal;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static net.syncthing.java.core.security.KeystoreHandler.*;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author aleph
 */
public final class BlockExchangeConnectionHandler implements Closeable {

    private final static int MAGIC = 0x2EA7D90B;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ConfigurationService configuration;

    private final ExecutorService outExecutorService = Executors.newSingleThreadExecutor(),
        inExecutorService = Executors.newSingleThreadExecutor(),
        messageProcessingService = Executors.newCachedThreadPool();
    private final ScheduledExecutorService periodicExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final EventBus eventBus = new EventBus();
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private final DeviceAddress address;
    private long lastActive = Long.MIN_VALUE;
    private ClusterConfigInfo clusterConfigInfo;
    private IndexHandler indexHandler;
    private boolean isClosed = false, isConnected = false;

    public BlockExchangeConnectionHandler(ConfigurationService configuration, DeviceAddress deviceAddress) {
        checkNotNull(configuration);
        this.configuration = configuration;
        this.address = deviceAddress;
    }

    public DeviceAddress getAddress() {
        return address;
    }

    public ClusterConfigInfo getClusterConfigInfo() {
        return clusterConfigInfo;
    }

    public IndexHandler getIndexHandler() {
        return indexHandler;
    }

    public void setIndexHandler(IndexHandler indexHandler) {
        checkNotClosed();
        this.indexHandler = indexHandler;
    }

    public void checkNotClosed() {
        checkArgument(!isClosed(), "connection %s closed", this);
    }

    public boolean isConnected() {
        return isConnected;
    }

    public BlockExchangeConnectionHandler connect() throws IOException, KeystoreHandler.CryptoException {
        checkNotClosed();
        checkArgument(socket == null && !isConnected, "already connected!");
        logger.info("connecting to {}", address.getAddress());

        KeystoreHandler keystoreHandler = KeystoreHandler.newLoader().loadAndStore(configuration);

        switch (address.getType()) {
            case TCP:
                logger.debug("opening tcp ssl connection");
                socket = keystoreHandler.createSocket(address.getSocketAddress(), BEP);
                break;
            case RELAY: {
                logger.debug("opening relay connection");
                socket = keystoreHandler.wrapSocket(new RelayClient(configuration).openRelayConnection(address), BEP);
                break;
            }
            case HTTP_RELAY:
            case HTTPS_RELAY: {
                logger.debug("opening http relay connection");
                socket = keystoreHandler.wrapSocket(new HttpRelayClient().openRelayConnection(address), BEP);
                break;
            }
            default:
                throw new UnsupportedOperationException("unsupported address type = " + address.getType());
        }
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());

        sendHelloMessage(BlockExchangeProtos.Hello.newBuilder()
            .setClientName(configuration.getClientName())
            .setClientVersion(configuration.getClientVersion())
            .setDeviceName(configuration.getDeviceName())
            .build().toByteArray());
        markActivityOnSocket();

        BlockExchangeProtos.Hello hello = receiveHelloMessage();
        logger.trace("received hello message = {}", hello);
        ConnectionInfo connectionInfo = new ConnectionInfo();
        connectionInfo.setClientName(hello.getClientName());
        connectionInfo.setClientVersion(hello.getClientVersion());
        connectionInfo.setDeviceName(hello.getDeviceName());
        logger.info("connected to device = {}", connectionInfo);
        try {
            keystoreHandler.checkSocketCerificate((SSLSocket) socket, address.getDeviceId());
        } catch (CertificateException e) {
            throw new IOException(e);
        }
        {
            ClusterConfig.Builder clusterConfigBuilder = ClusterConfig.newBuilder();
            for (String folder : configuration.getFolderNames()) {
                Folder.Builder folderBuilder = clusterConfigBuilder.addFoldersBuilder().setId(folder);
                {
                    //our device
                    Device.Builder deviceBuilder = folderBuilder.addDevicesBuilder()
                        .setId(ByteString.copyFrom(deviceIdStringToHashData(configuration.getDeviceId())));
                    if (indexHandler != null) {
                        deviceBuilder.setIndexId(indexHandler.getSequencer().indexId())
                            .setMaxSequence(indexHandler.getSequencer().currentSequence());
                    }
                }
                {
                    //other device
                    Device.Builder deviceBuilder = folderBuilder.addDevicesBuilder()
                        .setId(ByteString.copyFrom(deviceIdStringToHashData(address.getDeviceId())));
                    if (indexHandler != null) {
                        IndexInfo indexSequenceInfo = indexHandler.getIndexRepository().findIndexInfoByDeviceAndFolder(address.getDeviceId(), folder);
                        if (indexSequenceInfo != null) {
                            deviceBuilder
                                .setIndexId(indexSequenceInfo.getIndexId())
                                .setMaxSequence(indexSequenceInfo.getLocalSequence());
                            logger.info("send delta index info device = {} index = {} max (local) sequence = {}",
                                indexSequenceInfo.getDeviceId(),
                                indexSequenceInfo.getIndexId(),
                                indexSequenceInfo.getLocalSequence());
                        }
                    }
                }
                //TODO other devices??
            }
            sendMessage(clusterConfigBuilder.build());
        }
        final Object clusterConfigWaitingLock = new Object();
        synchronized (clusterConfigWaitingLock) {
            Object listener = new Object() {
                @Subscribe
                public void handleClusterConfigMessageProcessedEvent(ClusterConfigMessageProcessedEvent event) {
                    synchronized (clusterConfigWaitingLock) {
                        clusterConfigWaitingLock.notifyAll();
                    }
                }

                @Subscribe
                public void handleConnectionClosedEvent(ConnectionClosedEvent event) {
                    synchronized (clusterConfigWaitingLock) {
                        clusterConfigWaitingLock.notifyAll();
                    }
                }
            };
            eventBus.register(listener);
            startMessageListenerService();
            while (clusterConfigInfo == null && !isClosed()) {
                logger.debug("wait for cluster config");
                try {
                    clusterConfigWaitingLock.wait();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
            checkNotNull(clusterConfigInfo, "unable to retrieve cluster config from peer!");
            eventBus.unregister(listener);
        }
        for (String folder : configuration.getFolderNames()) {
            if (hasFolder(folder)) {
                sendIndexMessage(folder);
            }
        }
        periodicExecutorService.scheduleWithFixedDelay(this::sendPing, 90, 90, TimeUnit.SECONDS);
        isConnected = true;
        return this;
    }

    private void sendIndexMessage(String folder) {
        sendMessage(Index.newBuilder()
            .setFolder(folder)
            .build());
    }

    private void closeBg() {
        new Thread(this::close).start();
    }

    private BlockExchangeProtos.Hello receiveHelloMessage() throws IOException {
        logger.trace("receiving hello message");
        int magic = in.readInt();
        checkArgument(magic == MAGIC, "magic mismatch, expected %s, got %s", MAGIC, magic);
        int length = in.readShort();
        checkArgument(length > 0, "invalid lenght, must be >0, got %s", length);
        byte[] buffer = new byte[length];
        in.readFully(buffer);
        logger.trace("received hello message");
        return BlockExchangeProtos.Hello.parseFrom(buffer);
    }

    private Future sendHelloMessage(final byte[] payload) {
        return outExecutorService.submit(() -> {
            try {
                logger.trace("sending message");
                ByteBuffer header = ByteBuffer.allocate(6);
                header.putInt(MAGIC);
                header.putShort((short) payload.length);
                out.write(header.array());
                out.write(payload);
                out.flush();
                logger.trace("sent message");
            } catch (IOException ex) {
                if (outExecutorService.isShutdown()) {
                    return;
                }
                logger.error("error writing to output stream", ex);
                closeBg();
            }
        });
    }

    private Future sendPing() {
        return sendMessage(Ping.newBuilder().build());
    }

    /**
     * test connection, throw exception if failed
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void testConnection() throws InterruptedException, ExecutionException {
        checkNotClosed();
        sendPing().get();
    }

    private final static BiMap<BlockExchangeProtos.MessageType, Class<? extends GeneratedMessage>> messageTypes = ImmutableBiMap.<BlockExchangeProtos.MessageType, Class<? extends GeneratedMessage>>builder()
        .put(BlockExchangeProtos.MessageType.CLOSE, BlockExchangeProtos.Close.class)
        .put(BlockExchangeProtos.MessageType.CLUSTER_CONFIG, BlockExchangeProtos.ClusterConfig.class)
        .put(BlockExchangeProtos.MessageType.DOWNLOAD_PROGRESS, BlockExchangeProtos.DownloadProgress.class)
        .put(BlockExchangeProtos.MessageType.INDEX, BlockExchangeProtos.Index.class)
        .put(BlockExchangeProtos.MessageType.INDEX_UPDATE, BlockExchangeProtos.IndexUpdate.class)
        .put(BlockExchangeProtos.MessageType.PING, BlockExchangeProtos.Ping.class)
        .put(BlockExchangeProtos.MessageType.REQUEST, BlockExchangeProtos.Request.class)
        .put(BlockExchangeProtos.MessageType.RESPONSE, BlockExchangeProtos.Response.class)
        .build();

    private void markActivityOnSocket() {
        lastActive = System.currentTimeMillis();
    }

    private Pair<BlockExchangeProtos.MessageType, GeneratedMessage> receiveMessage() throws IOException {
        logger.trace("receiving message");
        int headerLength = in.readShort();
        while (headerLength == 0) {
            logger.warn("got headerLength == 0, skipping short");
            headerLength = in.readShort();
        }
        markActivityOnSocket();
        checkArgument(headerLength > 0, "invalid lenght, must be >0, got %s", headerLength);
        byte[] headerBuffer = new byte[headerLength];
        in.readFully(headerBuffer);
        BlockExchangeProtos.Header header = BlockExchangeProtos.Header.parseFrom(headerBuffer);
        logger.trace("message type = {} compression = {}", header.getType(), header.getCompression());
        int messageLength;
        while ((messageLength = in.readInt()) == 0) {
            logger.warn("received readInt() == 0, expecting 'bep message header length' (int >0), ignoring (keepalive?)");
        }
        checkArgument(messageLength >= 0, "invalid lenght, must be >=0, got %s", messageLength);
        byte[] messageBuffer = new byte[messageLength];
        in.readFully(messageBuffer);
        markActivityOnSocket();
        if (equal(header.getCompression(), BlockExchangeProtos.MessageCompression.LZ4)) {
            int uncompressedLength = ByteBuffer.wrap(messageBuffer).getInt();
            messageBuffer = LZ4Factory.fastestInstance().fastDecompressor().decompress(messageBuffer, 4, uncompressedLength);
        }
        checkArgument(messageTypes.containsKey(header.getType()), "unsupported message type = %s", header.getType());
        try {
            GeneratedMessage message = (GeneratedMessage) messageTypes.get(header.getType()).getMethod("parseFrom", byte[].class).invoke(null, (Object) messageBuffer);
            return Pair.of(header.getType(), message);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Future sendMessage(final GeneratedMessage message) {
        checkNotClosed();
        checkArgument(messageTypes.containsValue(message.getClass()));
        final BlockExchangeProtos.Header header = BlockExchangeProtos.Header.newBuilder()
            .setCompression(BlockExchangeProtos.MessageCompression.NONE)
            .setType(messageTypes.inverse().get(message.getClass()))
            .build();
        final byte[] headerData = header.toByteArray(), messageData = message.toByteArray(); //TODO compression
        return outExecutorService.submit(() -> {
            try {
                logger.debug("sending message type = {} {}", header.getType(), getIdForMessage(message));
                logger.trace("sending message = {}", message);
                markActivityOnSocket();
                out.writeShort(headerData.length);
                out.write(headerData);
                out.writeInt(messageData.length);//with compression, check this
                out.write(messageData);
                out.flush();
                markActivityOnSocket();
                logger.debug("sent message {}", getIdForMessage(message));
            } catch (IOException ex) {
                if (!outExecutorService.isShutdown()) {
                    logger.error("error writing to output stream", ex);
                    closeBg();
                }
                throw ex;
            }
            return null;
        });
    }

    @Override
    public void close() {
        if (!isClosed()) {
            isClosed = true;
            periodicExecutorService.shutdown();
            outExecutorService.shutdown();
            inExecutorService.shutdown();
            messageProcessingService.shutdown();
            if (out != null) {
                IOUtils.closeQuietly(out);
                out = null;
            }
            if (in != null) {
                IOUtils.closeQuietly(in);
                in = null;
            }
            if (socket != null) {
                IOUtils.closeQuietly(socket);
                socket = null;
            }
            logger.info("closed connection {}", address);
            eventBus.post(ConnectionClosedEvent.INSTANCE);
            try {
                periodicExecutorService.awaitTermination(2, TimeUnit.SECONDS);
                outExecutorService.awaitTermination(2, TimeUnit.SECONDS);
                inExecutorService.awaitTermination(2, TimeUnit.SECONDS);
                messageProcessingService.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                logger.warn("", ex);
            }
        }
    }

    /**
     * return time elapsed since last activity on socket, in millis
     *
     * @return
     */
    public long getLastActive() {
        return System.currentTimeMillis() - lastActive;
    }

    public EventBus getEventBus() {
        return eventBus;
    }

    /**
     * get id for message bean/instance, for log tracking
     *
     * @param message
     * @return id for message bean
     */
    private static String getIdForMessage(GeneratedMessage message) {
        if (message instanceof Request) {
            return Integer.toString(((Request) message).getId());
        } else if (message instanceof Response) {
            return Integer.toString(((Response) message).getId());
        } else {
            return Integer.toString(Math.abs(message.hashCode()));
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    private void startMessageListenerService() {
        inExecutorService.submit(() -> {
            try {
                while (!Thread.interrupted()) {
                    final Pair<BlockExchangeProtos.MessageType, GeneratedMessage> message = receiveMessage();
                    logger.debug("received message type = {} {}", message.getLeft(), getIdForMessage(message.getRight()));
                    logger.trace("received message = {}", message.getRight());
                    messageProcessingService.submit(() -> {
                        logger.debug("processing message type = {} {}", message.getLeft(), getIdForMessage(message.getRight()));
                        switch (message.getLeft()) {
                            case INDEX:
                                eventBus.post(new IndexMessageReceivedEvent((Index) message.getValue()));
                                break;
                            case INDEX_UPDATE:
                                eventBus.post(new IndexUpdateMessageReceivedEvent((IndexUpdate) message.getValue()));
                                break;
                            case REQUEST:
                                eventBus.post(new RequestMessageReceivedEvent((Request) message.getValue()));
                                break;
                            case RESPONSE:
                                eventBus.post(new ResponseMessageReceivedEvent((Response) message.getValue()));
                                break;
                            case PING:
                                logger.debug("ping message received");
                                break;
                            case CLOSE:
                                logger.info("received close message = {}", message.getValue());
                                closeBg();
                                break;
                            case CLUSTER_CONFIG: {
                                checkArgument(clusterConfigInfo == null, "received cluster config message twice!");
                                clusterConfigInfo = new ClusterConfigInfo();
                                ClusterConfig clusterConfig = (ClusterConfig) message.getValue();
                                for (Folder folder : firstNonNull(clusterConfig.getFoldersList(), Collections.<Folder>emptyList())) {
                                    ClusterConfigFolderInfo.Builder builder = ClusterConfigFolderInfo.newBuilder()
                                        .setFolder(folder.getId())
                                        .setLabel(folder.getLabel());
                                    Map<String, Device> devicesById = Maps.uniqueIndex(firstNonNull(folder.getDevicesList(), Collections.emptyList()),
                                            input -> hashDataToDeviceIdString(input.getId().toByteArray()));
                                    Device otherDevice = devicesById.get(address.getDeviceId()),
                                        ourDevice = devicesById.get(configuration.getDeviceId());
                                    if (otherDevice != null) {
                                        builder.setAnnounced(true);
                                    }
                                    final ClusterConfigFolderInfo folderInfo;
                                    if (ourDevice != null) {
                                        folderInfo = builder.setShared(true).build();
                                        logger.info("folder shared from device = {} folder = {}", address.getDeviceId(), folderInfo);
                                        if (!configuration.getFolderNames().contains(folderInfo.getFolder())) {
                                            configuration.edit().addFolders(new FolderInfo(folderInfo.getFolder(), folderInfo.getLabel()));
                                            logger.info("new folder shared = {}", folderInfo);
                                            eventBus.post(new NewFolderSharedEvent() {
                                                @Override
                                                public String getFolder() {
                                                    return folderInfo.getFolder();
                                                }

                                            });
                                        }
                                    } else {
                                        folderInfo = builder.build();
                                        logger.info("folder not shared from device = {} folder = {}", address.getDeviceId(), folderInfo);
                                    }
                                    clusterConfigInfo.putFolderInfo(folderInfo);
                                    configuration.edit().addPeers(Iterables.filter(Iterables.transform(firstNonNull(folder.getDevicesList(), Collections.emptyList()), device -> {
                                        String deviceId = hashDataToDeviceIdString(device.getId().toByteArray()),
                                            name = device.hasName() ? device.getName() : null;
                                        return new DeviceInfo(deviceId, name);
                                    }), s -> !equal(s.getDeviceId(), configuration.getDeviceId())));
                                }
                                configuration.edit().persistLater();
                                eventBus.post(new ClusterConfigMessageProcessedEvent(clusterConfig));
                            }
                            break;
                        }
                    });
                }
            } catch (IOException ex) {
                if (inExecutorService.isShutdown()) {
                    return;
                }
                logger.error("error receiving message", ex);
                closeBg();
            }
        });
    }

    public String getDeviceId() {
        return getAddress().getDeviceId();
    }

    public abstract class MessageReceivedEvent<E> implements DeviceAddressActiveEvent {

        private final E message;

        private MessageReceivedEvent(E message) {
            checkNotNull(message);
            this.message = message;
        }

        public E getMessage() {
            return message;
        }

        public BlockExchangeConnectionHandler getConnectionHandler() {
            return BlockExchangeConnectionHandler.this;
        }

        @Override
        public DeviceAddress getDeviceAddress() {
            return getConnectionHandler().getAddress();
        }

    }

    public abstract class AnyIndexMessageReceivedEvent<E> extends MessageReceivedEvent<E> {

        private AnyIndexMessageReceivedEvent(E message) {
            super(message);
        }

        public abstract List<BlockExchangeProtos.FileInfo> getFilesList();

        public abstract String getFolder();
    }

    public final class IndexMessageReceivedEvent extends AnyIndexMessageReceivedEvent<Index> {

        private IndexMessageReceivedEvent(Index message) {
            super(message);
        }

        @Override
        public List<BlockExchangeProtos.FileInfo> getFilesList() {
            return getMessage().getFilesList();
        }

        @Override
        public String getFolder() {
            return getMessage().getFolder();
        }

    }

    public final class IndexUpdateMessageReceivedEvent extends AnyIndexMessageReceivedEvent<IndexUpdate> {

        private IndexUpdateMessageReceivedEvent(IndexUpdate message) {
            super(message);
        }

        @Override
        public List<BlockExchangeProtos.FileInfo> getFilesList() {
            return getMessage().getFilesList();
        }

        @Override
        public String getFolder() {
            return getMessage().getFolder();
        }

    }

    public final class RequestMessageReceivedEvent extends MessageReceivedEvent<Request> {

        private RequestMessageReceivedEvent(Request message) {
            super(message);
        }

    }

    public final class ResponseMessageReceivedEvent extends MessageReceivedEvent<Response> {

        private ResponseMessageReceivedEvent(Response message) {
            super(message);
        }

    }

    public final class ClusterConfigMessageProcessedEvent extends MessageReceivedEvent<ClusterConfig> {

        private ClusterConfigMessageProcessedEvent(ClusterConfig message) {
            super(message);
        }

    }

    public enum ConnectionClosedEvent {
        INSTANCE
    }

    @Override
    public String toString() {
        return "BlockExchangeConnectionHandler{" + "address=" + address + ", lastActive=" + (getLastActive() / 1000d) + "secs ago}";
    }

    private static final class ConnectionInfo {

        private String deviceName, clientName, clientVersion;

        public String getDeviceName() {
            return deviceName;
        }

        public void setDeviceName(String deviceName) {
            this.deviceName = deviceName;
        }

        public String getClientName() {
            return clientName;
        }

        public void setClientName(String clientName) {
            this.clientName = clientName;
        }

        public String getClientVersion() {
            return clientVersion;
        }

        public void setClientVersion(String clientVersion) {
            this.clientVersion = clientVersion;
        }

        @Override
        public String toString() {
            return "ConnectionInfo{" + "deviceName=" + deviceName + ", clientName=" + clientName + ", clientVersion=" + clientVersion + '}';
        }

    }

    public final class ClusterConfigInfo {

        private final Map<String, ClusterConfigFolderInfo> folderInfoById = Maps.newConcurrentMap();

        public ClusterConfigFolderInfo getFolderInfo(String folderId) {
            ClusterConfigFolderInfo folderInfo = folderInfoById.get(folderId);
            if (folderInfo == null) {
                folderInfo = ClusterConfigFolderInfo.newBuilder().setFolder(folderId).build();
                folderInfoById.put(folderId, folderInfo);
            }
            return folderInfo;
        }

        private void putFolderInfo(ClusterConfigFolderInfo folderInfo) {
            folderInfoById.put(folderInfo.getFolder(), folderInfo);
        }

        public Set<String> getSharedFolders() {
            return Sets.newTreeSet(Iterables.transform(Iterables.filter(folderInfoById.values(), ClusterConfigFolderInfo::isShared), ClusterConfigFolderInfo::getFolder));
        }

    }

    public boolean hasFolder(String folder) {
        return getClusterConfigInfo().getSharedFolders().contains(folder);
    }

    public abstract class NewFolderSharedEvent {

        public abstract String getFolder();
    }

}
