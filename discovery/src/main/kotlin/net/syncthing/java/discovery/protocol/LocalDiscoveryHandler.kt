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

import com.google.common.eventbus.AsyncEventBus
import com.google.common.eventbus.Subscribe
import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.events.DeviceAddressReceivedEvent
import net.syncthing.java.core.security.KeystoreHandler
import net.syncthing.java.core.utils.NetworkUtils
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

internal class LocalDiscoveryHandler(private val configuration: ConfigurationService) : Closeable {

    companion object {
        private val MAGIC = 0x2EA7D90B
        private val LISTENING_PORT = 21027
        private val MAX_WAIT = 60 * 1000
        private val INCOMING_BUFFER_SIZE = 1024
    }

    private val logger = LoggerFactory.getLogger(javaClass)
    private val listeningExecutor = Executors.newSingleThreadScheduledExecutor()
    private val processingExecutor = Executors.newCachedThreadPool()
    val eventBus = AsyncEventBus(processingExecutor)
    private val localDiscoveryRecords = mutableMapOf<String, List<DeviceAddress>>()

    private var datagramSocket: DatagramSocket? = null

    fun queryAndClose(deviceId: String): Collection<DeviceAddress> {
        val lock = Object()
        eventBus.register(object : Any() {
            @Subscribe
            fun handleMessageReceivedEvent(event: MessageReceivedEvent) {
                synchronized(lock) {
                    if (deviceId == event.deviceId()) {
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
                logger.warn("", ex)
            }

        }
        close()
        synchronized(localDiscoveryRecords) {
            return localDiscoveryRecords[deviceId] ?: listOf()
        }
    }

    fun sendAnnounceMessage() {
        processingExecutor.submit {
            try {
                val out = ByteArrayOutputStream()
                DataOutputStream(out).writeInt(MAGIC)
                Announce.newBuilder()
                        .setId(ByteString.copyFrom(KeystoreHandler.deviceIdStringToHashData(configuration.deviceId!!)))
                        .setInstanceId(configuration.instanceId)
                        .build().writeTo(out)
                val data = out.toByteArray()
                val networkInterfaces = NetworkInterface.getNetworkInterfaces()
                while (networkInterfaces.hasMoreElements()) {
                    val networkInterface = networkInterfaces.nextElement()
                    for (interfaceAddress in networkInterface.interfaceAddresses) {
                        val broadcastAddress = interfaceAddress.broadcast
                        logger.trace("interface = {} address = {} broadcast = {}", networkInterface, interfaceAddress, broadcastAddress)
                        if (broadcastAddress != null) {
                            logger.debug("sending broadcast announce on {}", broadcastAddress)
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
                logger.warn("Failed to send local announce message", e)
            }
        }
    }

    fun startListener() {
        if (datagramSocket == null || datagramSocket!!.isClosed) {
            try {
                datagramSocket = DatagramSocket(LISTENING_PORT, InetAddress.getByName("0.0.0.0"))
                logger.info("Opened udp socket {}", datagramSocket!!.localSocketAddress)
            } catch (e: IOException) {
                logger.warn("Failed to open listening socket on port {}", LISTENING_PORT, e)
                return
            }

        }

        listeningExecutor.submit(object : Runnable {
            override fun run() {
                try {
                    val datagramPacket = DatagramPacket(ByteArray(INCOMING_BUFFER_SIZE), INCOMING_BUFFER_SIZE)
                    logger.trace("waiting for message on socket addr = {}",
                            datagramSocket!!.localSocketAddress)
                    datagramSocket!!.receive(datagramPacket)
                    processingExecutor.submit { handleReceivedDatagram(datagramPacket) }
                    listeningExecutor.submit(this)
                } catch (e: IOException) {
                    if (e.message == "Socket closed") {
                        // Ignore exception on socket close.
                        return
                    }
                    logger.warn("Error receiving datagram", e)
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
            NetworkUtils.assertProtocol(magic == MAGIC, {"magic mismatch, expected $MAGIC, got $magic"})
            val announce = Announce.parseFrom(ByteString.copyFrom(byteBuffer))
            val deviceId = KeystoreHandler.hashDataToDeviceIdString(announce.id.toByteArray())

            // Ignore announcement received from ourselves.
            if (deviceId == configuration.deviceId)
                return

            if (!configuration.getPeerIds().contains(deviceId)) {
                logger.trace("Received local announce from $deviceId which is not a peer, ignoring")
                return
            }

            logger.debug("received local announce from device id = {}", deviceId)
            val addressesList = announce.addressesList ?: listOf<String>()
            val deviceAddresses = addressesList.map { address ->
                // When interpreting addresses with an unspecified address, e.g.,
                // tcp://0.0.0.0:22000 or tcp://:42424, the source address of the
                // discovery announcement is to be used.
                DeviceAddress.Builder()
                        .setAddress(address.replaceFirst("tcp://(0.0.0.0|):".toRegex(), "tcp://$sourceAddress:"))
                        .setDeviceId(deviceId)
                        .setInstanceId(announce.instanceId)
                        .setProducer(DeviceAddress.AddressProducer.LOCAL_DISCOVERY)
                        .build()
            }
            var isNew = false
            synchronized(localDiscoveryRecords) {
                isNew = localDiscoveryRecords.remove(deviceId)?.isEmpty() == false
                localDiscoveryRecords.put(deviceId, deviceAddresses)
            }
            eventBus.post(object : MessageReceivedEvent() {

                override fun deviceId() = deviceId

                override fun getDeviceAddresses(): List<DeviceAddress> {
                    return Collections.unmodifiableList(deviceAddresses)
                }
            })
            if (isNew) {
                eventBus.post(object : NewLocalPeerEvent() {
                    override fun deviceId() = deviceId
                })
            }
        } catch (ex: InvalidProtocolBufferException) {
            logger.warn("error processing datagram", ex)
        }

    }

    override fun close() {
        processingExecutor.shutdown()
        listeningExecutor.shutdown()
        if (datagramSocket != null) {
            IOUtils.closeQuietly(datagramSocket)
        }
    }

    abstract inner class MessageReceivedEvent : DeviceAddressReceivedEvent {

        abstract fun deviceId(): String

        abstract override fun getDeviceAddresses(): List<DeviceAddress>
    }

    abstract inner class NewLocalPeerEvent {

        abstract fun deviceId(): String
    }
}
