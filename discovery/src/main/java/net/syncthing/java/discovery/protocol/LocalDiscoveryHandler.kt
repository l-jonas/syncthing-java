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
package net.syncthing.java.discovery.protocol

import com.google.common.base.Preconditions.checkArgument
import com.google.common.collect.HashMultimap
import com.google.common.eventbus.AsyncEventBus
import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.events.DeviceAddressReceivedEvent
import net.syncthing.java.core.security.KeystoreHandler
import net.syncthing.java.discovery.protocol.LocalDiscoveryProtos.Announce
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.DataOutputStream
import java.io.IOException
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.NetworkInterface
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.Executors

internal class LocalDiscoveryHandler(private val mConfiguration: ConfigurationService) : Closeable {

    companion object {
        private val MAGIC = 0x2EA7D90B
        private val LISTENING_PORT = 21027
        private val MAX_WAIT = 60 * 1000
        private val INCOMING_BUFFER_SIZE = 1024
    }

    private val mLogger = LoggerFactory.getLogger(javaClass)
    private val mListeningExecutor = Executors.newSingleThreadScheduledExecutor()
    private val mProcessingExecutor = Executors.newCachedThreadPool()
    val eventBus: EventBus = AsyncEventBus(mProcessingExecutor)
    private val mLocalDiscoveryRecords = HashMultimap.create<String, DeviceAddress>()

    private var mDatagramSocket: DatagramSocket? = null

    fun queryAndClose(deviceId: String): Collection<DeviceAddress> {
        val lock = Object()
        eventBus.register(object : Any() {
            @Subscribe
            fun handleMessageReceivedEvent(event: MessageReceivedEvent) {
                synchronized(lock) {
                    if (deviceId == event.deviceId) {
                        lock.notify()
                    }
                }
            }
        })
        startListener()
        sendAnnounceMessage()
        synchronized(lock) {
            try {
                lock.wait(MAX_WAIT.toLong())
            } catch (ex: InterruptedException) {
                mLogger.warn("", ex)
            }

        }
        close()
        synchronized(mLocalDiscoveryRecords) {
            return mLocalDiscoveryRecords.get(deviceId)
        }
    }

    fun sendAnnounceMessage() {
        mProcessingExecutor.submit {
            try {
                val out = ByteArrayOutputStream()
                DataOutputStream(out).writeInt(MAGIC)
                Announce.newBuilder()
                        .setId(ByteString.copyFrom(KeystoreHandler.deviceIdStringToHashData(mConfiguration.deviceId)))
                        .setInstanceId(mConfiguration.instanceId)
                        .build().writeTo(out)
                val data = out.toByteArray()
                val networkInterfaces = NetworkInterface.getNetworkInterfaces()
                while (networkInterfaces.hasMoreElements()) {
                    val networkInterface = networkInterfaces.nextElement()
                    for (interfaceAddress in networkInterface.interfaceAddresses) {
                        val broadcastAddress = interfaceAddress.broadcast
                        mLogger.trace("interface = {} address = {} broadcast = {}", networkInterface, interfaceAddress, broadcastAddress)
                        if (broadcastAddress != null) {
                            mLogger.debug("sending broadcast announce on {}", broadcastAddress)
                            DatagramSocket().use { broadcastSocket ->
                                broadcastSocket.broadcast = true
                                val datagramPacket = DatagramPacket(
                                        data, data.size, broadcastAddress, LISTENING_PORT)
                                broadcastSocket.send(datagramPacket)
                            }
                        }
                    }
                }
            } catch (e: IOException) {
                mLogger.warn("Failed to send local announce message", e)
            }
        }
    }

    fun startListener() {
        if (mDatagramSocket == null || mDatagramSocket!!.isClosed) {
            try {
                mDatagramSocket = DatagramSocket(LISTENING_PORT, InetAddress.getByName("0.0.0.0"))
                mLogger.info("Opened udp socket {}", mDatagramSocket!!.localSocketAddress)
            } catch (e: IOException) {
                mLogger.warn("Failed to open listening socket on port {}", LISTENING_PORT, e)
                return
            }

        }

        mListeningExecutor.submit(object : Runnable {
            override fun run() {
                try {
                    val datagramPacket = DatagramPacket(ByteArray(INCOMING_BUFFER_SIZE), INCOMING_BUFFER_SIZE)
                    mLogger.trace("waiting for message on socket addr = {}",
                            mDatagramSocket!!.localSocketAddress)
                    mDatagramSocket!!.receive(datagramPacket)
                    mProcessingExecutor.submit { handleReceivedDatagram(datagramPacket) }
                    mListeningExecutor.submit(this)
                } catch (e: IOException) {
                    if (e.message == "Socket closed") {
                        // Ignore exception on socket close.
                        return
                    }
                    mLogger.warn("Error receiving datagram", e)
                    close()
                }

            }
        })
    }

    private fun handleReceivedDatagram(datagramPacket: DatagramPacket) {
        try {
            val sourceAddress = datagramPacket.address.hostAddress
            val byteBuffer = ByteBuffer.wrap(
                    datagramPacket.data, datagramPacket.offset, datagramPacket.length)
            val magic = byteBuffer.int
            checkArgument(magic == MAGIC, "magic mismatch, expected %s, got %s", MAGIC, magic)
            val announce = Announce.parseFrom(ByteString.copyFrom(byteBuffer))
            val deviceId = KeystoreHandler.hashDataToDeviceIdString(announce.id.toByteArray())

            // Ignore announcement received from ourselves.
            if (deviceId == mConfiguration.deviceId)
                return

            mLogger.debug("received local announce from device id = {}", deviceId)
            val addressesList = announce.addressesList ?: listOf<String>()
            val deviceAddresses = addressesList.map { address ->
                // When interpreting addresses with an unspecified address, e.g.,
                // tcp://0.0.0.0:22000 or tcp://:42424, the source address of the
                // discovery announcement is to be used.
                DeviceAddress.newBuilder()
                        .setAddress(address?.replaceFirst("tcp://(0.0.0.0|):".toRegex(), "tcp://$sourceAddress:"))
                        .setDeviceId(deviceId)
                        .setInstanceId(announce.instanceId)
                        .setProducer(DeviceAddress.AddressProducer.LOCAL_DISCOVERY)
                        .build()
            }
            var isNew = false
            synchronized(mLocalDiscoveryRecords) {
                isNew = !mLocalDiscoveryRecords.removeAll(deviceId).isEmpty()
                mLocalDiscoveryRecords.putAll(deviceId, deviceAddresses)
            }
            eventBus.post(object : MessageReceivedEvent() {

                override val deviceId: String
                    get() = deviceId

                override fun getDeviceAddresses(): List<DeviceAddress> {
                    return Collections.unmodifiableList(deviceAddresses)
                }
            })
            if (isNew) {
                eventBus.post(object : NewLocalPeerEvent() {
                    override val deviceId: String
                        get() = deviceId
                })
            }
        } catch (ex: InvalidProtocolBufferException) {
            mLogger.warn("error processing datagram", ex)
        }

    }

    override fun close() {
        mProcessingExecutor.shutdown()
        mListeningExecutor.shutdown()
        if (mDatagramSocket != null) {
            IOUtils.closeQuietly(mDatagramSocket)
        }
    }

    abstract inner class MessageReceivedEvent : DeviceAddressReceivedEvent {

        abstract val deviceId: String

        abstract override fun getDeviceAddresses(): List<DeviceAddress>
    }

    abstract inner class NewLocalPeerEvent {

        abstract val deviceId: String
    }
}
