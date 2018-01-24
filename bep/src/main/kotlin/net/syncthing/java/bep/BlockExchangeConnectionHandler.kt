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
package net.syncthing.java.bep

import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.google.protobuf.ByteString
import com.google.protobuf.MessageLite
import net.jpountz.lz4.LZ4Factory
import net.syncthing.java.bep.BlockExchangeProtos.*
import net.syncthing.java.client.protocol.rp.RelayClient
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.events.DeviceAddressActiveEvent
import net.syncthing.java.core.security.KeystoreHandler
import net.syncthing.java.core.utils.NetworkUtils
import net.syncthing.java.httprelay.HttpRelayClient
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.tuple.Pair
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.net.Socket
import java.nio.ByteBuffer
import java.security.cert.CertificateException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLSocket

class BlockExchangeConnectionHandler(private val configuration: ConfigurationService, val address: DeviceAddress, private val indexHandler: IndexHandler) : Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val outExecutorService = Executors.newSingleThreadExecutor()
    private val inExecutorService = Executors.newSingleThreadExecutor()
    private val messageProcessingService = Executors.newCachedThreadPool()
    private val periodicExecutorService = Executors.newSingleThreadScheduledExecutor()
    val eventBus = EventBus()
    private var socket: Socket? = null
    private var inputStream: DataInputStream? = null
    private var outputStream: DataOutputStream? = null
    private var lastActive = Long.MIN_VALUE
    internal var clusterConfigInfo: ClusterConfigInfo? = null
    var isClosed = false
        private set
    var isConnected = false
        private set

    fun deviceId(): String = address.deviceId

    private fun checkNotClosed() {
        NetworkUtils.assertProtocol(!isClosed, {"connection $this closed"})
    }

    @Throws(IOException::class, KeystoreHandler.CryptoException::class)
    fun connect(): BlockExchangeConnectionHandler {
        checkNotClosed()
        assert(socket == null && !isConnected, {"already connected!"})
        logger.info("connecting to {}", address.address)

        val keystoreHandler = KeystoreHandler.Loader().loadAndStore(configuration)

        socket = when (address.getType()) {
            DeviceAddress.AddressType.TCP -> {
                logger.debug("opening tcp ssl connection")
                keystoreHandler.createSocket(address.getSocketAddress(), KeystoreHandler.BEP)
            }
            DeviceAddress.AddressType.RELAY -> {
                logger.debug("opening relay connection")
                keystoreHandler.wrapSocket(RelayClient(configuration).openRelayConnection(address), KeystoreHandler.BEP)
            }
            DeviceAddress.AddressType.HTTP_RELAY, DeviceAddress.AddressType.HTTPS_RELAY -> {
                logger.debug("opening http relay connection")
                keystoreHandler.wrapSocket(HttpRelayClient().openRelayConnection(address), KeystoreHandler.BEP)
            }
            else -> throw UnsupportedOperationException("unsupported address type = " + address.getType())
        }
        inputStream = DataInputStream(socket!!.getInputStream())
        outputStream = DataOutputStream(socket!!.getOutputStream())

        sendHelloMessage(BlockExchangeProtos.Hello.newBuilder()
                .setClientName(configuration.getClientName())
                .setClientVersion(configuration.clientVersion)
                .setDeviceName(configuration.deviceName)
                .build().toByteArray())
        markActivityOnSocket()

        val hello = receiveHelloMessage()
        logger.trace("received hello message = {}", hello)
        val connectionInfo = ConnectionInfo()
        connectionInfo.clientName = hello.clientName
        connectionInfo.clientVersion = hello.clientVersion
        connectionInfo.deviceName = hello.deviceName
        logger.info("connected to device = {}", connectionInfo)
        try {
            keystoreHandler.checkSocketCerificate((socket as SSLSocket?)!!, address.deviceId)
        } catch (e: CertificateException) {
            throw IOException(e)
        }

        run {
            val clusterConfigBuilder = ClusterConfig.newBuilder()
            for (folder in configuration.getFolderNames()) {
                val folderBuilder = Folder.newBuilder().setId(folder)
                run {
                    //our device
                    val deviceBuilder = Device.newBuilder()
                            .setId(ByteString.copyFrom(KeystoreHandler.deviceIdStringToHashData(configuration.deviceId!!)))
                            .setIndexId(indexHandler.sequencer().indexId())
                            .setMaxSequence(indexHandler.sequencer().currentSequence())
                    folderBuilder.addDevices(deviceBuilder)
                }
                run {
                    //other device
                    val deviceBuilder = Device.newBuilder()
                            .setId(ByteString.copyFrom(KeystoreHandler.deviceIdStringToHashData(address.deviceId)))
                    val indexSequenceInfo = indexHandler.indexRepository.findIndexInfoByDeviceAndFolder(address.deviceId, folder)
                    indexSequenceInfo?.let {
                        deviceBuilder
                                .setIndexId(indexSequenceInfo.indexId)
                                .setMaxSequence(indexSequenceInfo.localSequence)
                        logger.info("send delta index info device = {} index = {} max (local) sequence = {}",
                                indexSequenceInfo.deviceId,
                                indexSequenceInfo.indexId,
                                indexSequenceInfo.localSequence)
                    }
                    folderBuilder.addDevices(deviceBuilder)
                }
                clusterConfigBuilder.addFolders(folderBuilder)
                //TODO other devices??
            }
            sendMessage(clusterConfigBuilder.build())
        }
        val clusterConfigWaitingLock = Object()
        synchronized(clusterConfigWaitingLock) {
            val listener = object : Any() {
                @Subscribe
                fun handleClusterConfigMessageProcessedEvent(event: ClusterConfigMessageProcessedEvent) {
                    synchronized(clusterConfigWaitingLock) {
                        clusterConfigWaitingLock.notifyAll()
                    }
                }

                @Subscribe
                fun handleConnectionClosedEvent(event: ConnectionClosedEvent) {
                    synchronized(clusterConfigWaitingLock) {
                        clusterConfigWaitingLock.notifyAll()
                    }
                }
            }
            eventBus.register(listener)
            startMessageListenerService()
            while (clusterConfigInfo == null && !isClosed) {
                logger.debug("wait for cluster config")
                try {
                    clusterConfigWaitingLock.wait()
                } catch (e: InterruptedException) {
                    throw IOException(e)
                }

            }
            if (clusterConfigInfo == null) {
                throw IOException("unable to retrieve cluster config from peer!")
            }
            eventBus.unregister(listener)
        }
        for (folder in configuration.getFolderNames()) {
            if (hasFolder(folder)) {
                sendIndexMessage(folder)
            }
        }
        periodicExecutorService.scheduleWithFixedDelay({ this.sendPing() }, 90, 90, TimeUnit.SECONDS)
        isConnected = true
        return this
    }

    fun getBlockPuller(): BlockPuller {
        return BlockPuller(configuration, this)
    }

    fun getBlockPusher(): BlockPusher {
        return BlockPusher(configuration, this, indexHandler)
    }

    private fun sendIndexMessage(folder: String) {
        sendMessage(Index.newBuilder()
                .setFolder(folder)
                .build())
    }

    private fun closeBg() {
        Thread(Runnable { this.close() }).start()
    }

    @Throws(IOException::class)
    private fun receiveHelloMessage(): BlockExchangeProtos.Hello {
        logger.trace("receiving hello message")
        val magic = inputStream!!.readInt()
        NetworkUtils.assertProtocol(magic == MAGIC, {"magic mismatch, expected $MAGIC, got $magic"})
        val length = inputStream!!.readShort().toInt()
        NetworkUtils.assertProtocol(length > 0, {"invalid lenght, must be >0, got $length"})
        val buffer = ByteArray(length)
        inputStream!!.readFully(buffer)
        logger.trace("received hello message")
        return BlockExchangeProtos.Hello.parseFrom(buffer)
    }

    private fun sendHelloMessage(payload: ByteArray): Future<*> {
        return outExecutorService.submit {
            try {
                logger.trace("sending message")
                val header = ByteBuffer.allocate(6)
                header.putInt(MAGIC)
                header.putShort(payload.size.toShort())
                outputStream!!.write(header.array())
                outputStream!!.write(payload)
                outputStream!!.flush()
                logger.trace("sent message")
            } catch (ex: IOException) {
                if (outExecutorService.isShutdown) {
                    return@submit
                }
                logger.error("error writing to output stream", ex)
                closeBg()
            }
        }
    }

    private fun sendPing(): Future<*> {
        return sendMessage(Ping.newBuilder().build())
    }

    private fun markActivityOnSocket() {
        lastActive = System.currentTimeMillis()
    }

    @Throws(IOException::class)
    private fun receiveMessage(): Pair<BlockExchangeProtos.MessageType, MessageLite> {
        logger.trace("receiving message")
        var headerLength = inputStream!!.readShort().toInt()
        while (headerLength == 0) {
            logger.warn("got headerLength == 0, skipping short")
            headerLength = inputStream!!.readShort().toInt()
        }
        markActivityOnSocket()
        NetworkUtils.assertProtocol(headerLength > 0, {"invalid lenght, must be >0, got $headerLength"})
        val headerBuffer = ByteArray(headerLength)
        inputStream!!.readFully(headerBuffer)
        val header = BlockExchangeProtos.Header.parseFrom(headerBuffer)
        logger.trace("message type = {} compression = {}", header.type, header.compression)
        var messageLength = 0
        while (messageLength == 0) {
            logger.warn("received readInt() == 0, expecting 'bep message header length' (int >0), ignoring (keepalive?)")
            messageLength = inputStream!!.readInt()
        }
        NetworkUtils.assertProtocol(messageLength >= 0, {"invalid lenght, must be >=0, got $messageLength"})
        var messageBuffer = ByteArray(messageLength)
        inputStream!!.readFully(messageBuffer)
        markActivityOnSocket()
        if (header.compression == BlockExchangeProtos.MessageCompression.LZ4) {
            val uncompressedLength = ByteBuffer.wrap(messageBuffer).int
            messageBuffer = LZ4Factory.fastestInstance().fastDecompressor().decompress(messageBuffer, 4, uncompressedLength)
        }
        NetworkUtils.assertProtocol(messageTypes.containsKey(header.type), {"unsupported message type = ${header.type}"})
        try {
            val message = messageTypes[header.type]?.getMethod("parseFrom", ByteArray::class.java)?.invoke(null, messageBuffer as Any) as MessageLite
            return Pair.of(header.type, message)
        } catch (e: Exception) {
            when (e) {
                is IllegalAccessException, is IllegalArgumentException, is InvocationTargetException, is NoSuchMethodException, is SecurityException ->
                    throw RuntimeException(e)
                else -> throw e
            }
        }
    }

    internal fun sendMessage(message: MessageLite): Future<*> {
        checkNotClosed()
        assert(messageTypes.containsValue(message.javaClass))
        val header = BlockExchangeProtos.Header.newBuilder()
                .setCompression(BlockExchangeProtos.MessageCompression.NONE)
                // invert map
                .setType(messageTypes.entries.associateBy({ it.value }) { it.key }[message.javaClass])
                .build()
        val headerData = header.toByteArray()
        val messageData = message.toByteArray() //TODO compression
        return outExecutorService.submit<Any> {
            try {
                logger.debug("sending message type = {} {}", header.type, getIdForMessage(message))
                logger.trace("sending message = {}", message)
                markActivityOnSocket()
                outputStream!!.writeShort(headerData.size)
                outputStream!!.write(headerData)
                outputStream!!.writeInt(messageData.size)//with compression, check this
                outputStream!!.write(messageData)
                outputStream!!.flush()
                markActivityOnSocket()
                logger.debug("sent message {}", getIdForMessage(message))
            } catch (ex: IOException) {
                if (!outExecutorService.isShutdown) {
                    logger.error("error writing to output stream", ex)
                    closeBg()
                }
                throw ex
            }

            null
        }
    }

    override fun close() {
        if (!isClosed) {
            isClosed = true
            periodicExecutorService.shutdown()
            outExecutorService.shutdown()
            inExecutorService.shutdown()
            messageProcessingService.shutdown()
            if (outputStream != null) {
                IOUtils.closeQuietly(outputStream)
                outputStream = null
            }
            if (inputStream != null) {
                IOUtils.closeQuietly(inputStream)
                inputStream = null
            }
            if (socket != null) {
                IOUtils.closeQuietly(socket)
                socket = null
            }
            logger.info("closed connection {}", address)
            eventBus.post(ConnectionClosedEvent.INSTANCE)
            try {
                periodicExecutorService.awaitTermination(2, TimeUnit.SECONDS)
                outExecutorService.awaitTermination(2, TimeUnit.SECONDS)
                inExecutorService.awaitTermination(2, TimeUnit.SECONDS)
                messageProcessingService.awaitTermination(2, TimeUnit.SECONDS)
            } catch (ex: InterruptedException) {
                logger.warn("", ex)
            }

        }
    }

    /**
     * return time elapsed since last activity on socket, inputStream millis
     *
     * @return
     */
    fun getLastActive(): Long {
        return System.currentTimeMillis() - lastActive
    }

    private fun startMessageListenerService() {
        inExecutorService.submit {
            try {
                while (!Thread.interrupted()) {
                    val message = receiveMessage()
                    logger.debug("received message type = {} {}", message.left, getIdForMessage(message.right))
                    logger.trace("received message = {}", message.right)
                    messageProcessingService.submit {
                        logger.debug("processing message type = {} {}", message.left, getIdForMessage(message.right))
                        when (message.left) {
                            BlockExchangeProtos.MessageType.INDEX -> eventBus.post(IndexMessageReceivedEvent(message.value as Index))
                            BlockExchangeProtos.MessageType.INDEX_UPDATE -> eventBus.post(IndexUpdateMessageReceivedEvent(message.value as IndexUpdate))
                            BlockExchangeProtos.MessageType.REQUEST -> eventBus.post(RequestMessageReceivedEvent(message.value as Request))
                            BlockExchangeProtos.MessageType.RESPONSE -> eventBus.post(ResponseMessageReceivedEvent(message.value as Response))
                            BlockExchangeProtos.MessageType.PING -> logger.debug("ping message received")
                            BlockExchangeProtos.MessageType.CLOSE -> {
                                logger.info("received close message = {}", message.value)
                                closeBg()
                            }
                            BlockExchangeProtos.MessageType.CLUSTER_CONFIG -> {
                                NetworkUtils.assertProtocol(clusterConfigInfo == null, {"received cluster config message twice!"})
                                clusterConfigInfo = ClusterConfigInfo()
                                val clusterConfig = message.value as ClusterConfig
                                for (folder in clusterConfig.foldersList ?: emptyList()) {
                                    val folderInfo = ClusterConfigFolderInfo(folder.id, folder.label)
                                    val devicesById = (folder.devicesList ?: emptyList())
                                            .associateBy { input ->
                                                KeystoreHandler.hashDataToDeviceIdString(input.id!!.toByteArray())
                                            }
                                    val otherDevice = devicesById[address.deviceId]
                                    val ourDevice = devicesById[configuration.deviceId]
                                    if (otherDevice != null) {
                                        folderInfo.isAnnounced = true
                                    }
                                    if (ourDevice != null) {
                                        folderInfo.isShared = true
                                        logger.info("folder shared from device = {} folder = {}", address.deviceId, folderInfo)
                                        if (!configuration.getFolderNames().contains(folderInfo.folder)) {
                                            configuration.Editor().addFolders(setOf(FolderInfo(folderInfo.folder, folderInfo.label)))
                                            logger.info("new folder shared = {}", folderInfo)
                                            eventBus.post(object : NewFolderSharedEvent() {
                                                override val folder: String
                                                    get() = folderInfo.folder

                                            })
                                        }
                                    } else {
                                        logger.info("folder not shared from device = {} folder = {}", address.deviceId, folderInfo)
                                    }
                                    clusterConfigInfo!!.putFolderInfo(folderInfo)
                                }
                                configuration.Editor().persistLater()
                                eventBus.post(ClusterConfigMessageProcessedEvent(clusterConfig))
                            }
                        }
                    }
                }
            } catch (ex: IOException) {
                if (inExecutorService.isShutdown) {
                    return@submit
                }
                logger.error("error receiving message", ex)
                closeBg()
            }
        }
    }

    abstract inner class MessageReceivedEvent<E> constructor(val message: E) : DeviceAddressActiveEvent {

        val connectionHandler: BlockExchangeConnectionHandler
            get() = this@BlockExchangeConnectionHandler

        override fun getDeviceAddress(): DeviceAddress {
            return connectionHandler.address
        }

    }

    abstract inner class AnyIndexMessageReceivedEvent<E> constructor(message: E) : MessageReceivedEvent<E>(message) {

        abstract val filesList: List<BlockExchangeProtos.FileInfo>

        abstract val folder: String
    }

    inner class IndexMessageReceivedEvent constructor(message: Index) : AnyIndexMessageReceivedEvent<Index>(message) {

        override val filesList: List<BlockExchangeProtos.FileInfo>
            get() = message.filesList

        override val folder: String
            get() = message.folder

    }

    inner class IndexUpdateMessageReceivedEvent constructor(message: IndexUpdate) : AnyIndexMessageReceivedEvent<IndexUpdate>(message) {

        override val filesList: List<BlockExchangeProtos.FileInfo>
            get() = message.filesList

        override val folder: String
            get() = message.folder

    }

    inner class RequestMessageReceivedEvent constructor(message: Request) : MessageReceivedEvent<Request>(message)

    inner class ResponseMessageReceivedEvent constructor(message: Response) : MessageReceivedEvent<Response>(message)

    inner class ClusterConfigMessageProcessedEvent constructor(message: ClusterConfig) : MessageReceivedEvent<ClusterConfig>(message)

    enum class ConnectionClosedEvent {
        INSTANCE
    }

    override fun toString(): String {
        return "BlockExchangeConnectionHandler{" + "address=" + address + ", lastActive=" + getLastActive() / 1000.0 + "secs ago}"
    }

    private class ConnectionInfo {

        var deviceName: String? = null
        var clientName: String? = null
        var clientVersion: String? = null

        override fun toString(): String {
            return "ConnectionInfo{deviceName=$deviceName, clientName=$clientName, clientVersion=$clientVersion}"
        }

    }

    inner internal class ClusterConfigInfo {

        private val folderInfoById = ConcurrentHashMap<String, ClusterConfigFolderInfo>()

        val sharedFolders: Set<String>
            get() = folderInfoById.values.filter { it.isShared }.map { it.folder }.toSet()

        fun getFolderInfo(folderId: String): ClusterConfigFolderInfo {
            return folderInfoById[folderId] ?: let {
                val fi = ClusterConfigFolderInfo(folderId)
                folderInfoById.put(folderId, fi)
                fi
            }
        }

        fun putFolderInfo(folderInfo: ClusterConfigFolderInfo) {
            folderInfoById.put(folderInfo.folder, folderInfo)
        }

    }

    fun hasFolder(folder: String): Boolean {
        return clusterConfigInfo!!.sharedFolders.contains(folder)
    }

    abstract inner class NewFolderSharedEvent {

        abstract val folder: String
    }

    companion object {

        private val MAGIC = 0x2EA7D90B

        private val messageTypes: Map<MessageType, Class<out MessageLite>> = mapOf(
                BlockExchangeProtos.MessageType.CLOSE to BlockExchangeProtos.Close::class.java,
                BlockExchangeProtos.MessageType.CLUSTER_CONFIG to BlockExchangeProtos.ClusterConfig::class.java,
                BlockExchangeProtos.MessageType.DOWNLOAD_PROGRESS to BlockExchangeProtos.DownloadProgress::class.java,
                BlockExchangeProtos.MessageType.INDEX to BlockExchangeProtos.Index::class.java,
                BlockExchangeProtos.MessageType.INDEX_UPDATE to BlockExchangeProtos.IndexUpdate::class.java,
                BlockExchangeProtos.MessageType.PING to BlockExchangeProtos.Ping::class.java,
                BlockExchangeProtos.MessageType.REQUEST to BlockExchangeProtos.Request::class.java,
                BlockExchangeProtos.MessageType.RESPONSE to BlockExchangeProtos.Response::class.java)

        /**
         * get id for message bean/instance, for log tracking
         *
         * @param message
         * @return id for message bean
         */
        private fun getIdForMessage(message: MessageLite): String {
            return when (message) {
                is Request -> Integer.toString(message.id)
                is Response -> Integer.toString(message.id)
                else -> Integer.toString(Math.abs(message.hashCode()))
            }
        }
    }

}
