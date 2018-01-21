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
package net.syncthing.java.httprelay

import com.google.common.base.Strings
import com.google.common.collect.Queues
import com.google.protobuf.ByteString
import net.syncthing.java.core.interfaces.RelayConnection
import org.apache.http.HttpStatus
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.*
import java.net.*
import java.util.concurrent.*

import com.google.common.base.Objects.equal
import com.google.common.base.Preconditions.checkArgument
import com.google.common.base.Preconditions.checkNotNull

class HttpRelayConnection(private val httpRelayServerUrl: String, deviceId: String) : RelayConnection, Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val outgoingExecutorService = Executors.newSingleThreadExecutor()
    private val incomingExecutorService = Executors.newSingleThreadExecutor()
    private val flusherStreamService = Executors.newSingleThreadScheduledExecutor()
    private var peerToRelaySequence: Long = 0
    private var relayToPeerSequence: Long = 0
    private val sessionId: String
    private val incomingDataQueue = Queues.newLinkedBlockingQueue<ByteArray>()
    private val socket: Socket
    private val isServerSocket: Boolean
    private val inputStream: InputStream
    private val outputStream: OutputStream

    var isClosed = false
        private set

    override fun getSocket(): Socket {
        return socket
    }

    override fun isServerSocket(): Boolean {
        return isServerSocket
    }

    init {
        val serverMessage = sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder()
                .setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.CONNECT)
                .setDeviceId(deviceId))
        checkArgument(equal(serverMessage.messageType, HttpRelayProtos.HttpRelayServerMessageType.PEER_CONNECTED))
        checkNotNull<String>(Strings.emptyToNull(serverMessage.sessionId))
        sessionId = serverMessage.sessionId
        isServerSocket = serverMessage.isServerSocket
        outputStream = object : OutputStream() {

            private var buffer = ByteArrayOutputStream()
            private var lastFlush = System.currentTimeMillis()

            init {
                flusherStreamService.scheduleWithFixedDelay({
                    if (System.currentTimeMillis() - lastFlush > 1000) {
                        try {
                            flush()
                        } catch (ex: IOException) {
                            logger.warn("", ex)
                        }

                    }
                }, 1, 1, TimeUnit.SECONDS)
            }

            @Synchronized
            @Throws(IOException::class)
            override fun write(i: Int) {
                checkArgument(!this@HttpRelayConnection.isClosed)
                buffer.write(i)
            }

            @Synchronized
            @Throws(IOException::class)
            override fun write(bytes: ByteArray, offset: Int, size: Int) {
                checkArgument(!this@HttpRelayConnection.isClosed)
                buffer.write(bytes, offset, size)
            }

            @Synchronized
            @Throws(IOException::class)
            override fun flush() {
                val data = ByteString.copyFrom(buffer.toByteArray())
                buffer = ByteArrayOutputStream()
                try {
                    if (!data.isEmpty) {
                        outgoingExecutorService.submit {
                            sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder()
                                    .setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.PEER_TO_RELAY)
                                    .setSequence(++peerToRelaySequence)
                                    .setData(data))
                        }.get()
                    }
                    lastFlush = System.currentTimeMillis()
                } catch (ex: InterruptedException) {
                    logger.error("error", ex)
                    closeBg()
                    throw IOException(ex)
                } catch (ex: ExecutionException) {
                    logger.error("error", ex)
                    closeBg()
                    throw IOException(ex)
                }

            }

            @Synchronized
            @Throws(IOException::class)
            override fun write(bytes: ByteArray) {
                checkArgument(!this@HttpRelayConnection.isClosed)
                buffer.write(bytes)
            }

        }
        incomingExecutorService.submit {
            while (!isClosed) {
                val serverMessage1 = sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder().setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.WAIT_FOR_DATA))
                if (isClosed) {
                    return@submit
                }
                checkArgument(equal(serverMessage1.messageType, HttpRelayProtos.HttpRelayServerMessageType.RELAY_TO_PEER))
                checkArgument(serverMessage1.sequence == relayToPeerSequence + 1)
                if (!serverMessage1.data.isEmpty) {
                    incomingDataQueue.add(serverMessage1.data.toByteArray())
                }
                relayToPeerSequence = serverMessage1.sequence
            }
        }
        inputStream = object : InputStream() {

            private var noMoreData = false
            private var byteArrayInputStream = ByteArrayInputStream(ByteArray(0))

            @Throws(IOException::class)
            override fun read(): Int {
                checkArgument(!this@HttpRelayConnection.isClosed)
                if (noMoreData) {
                    return -1
                }
                var bite = -1
                while (bite == -1) {
                    bite = byteArrayInputStream.read()
                    try {
                        val data = incomingDataQueue.poll(1, TimeUnit.SECONDS)
                        if (data == null) {
                            //continue
                        } else if (data == STREAM_CLOSED) {
                            noMoreData = true
                            return -1
                        } else {
                            byteArrayInputStream = ByteArrayInputStream(data)
                        }
                    } catch (ex: InterruptedException) {
                        logger.warn("", ex)
                    }

                }
                return bite
            }

        }
        socket = object : Socket() {
            override fun isClosed(): Boolean {
                return this@HttpRelayConnection.isClosed
            }

            override fun isConnected(): Boolean {
                return !isClosed
            }

            @Throws(IOException::class)
            override fun shutdownOutput() {
                logger.debug("shutdownOutput")
                outputStream.flush()
            }

            @Throws(IOException::class)
            override fun shutdownInput() {
                logger.debug("shutdownInput")
                //do nothing
            }

            @Synchronized
            @Throws(IOException::class)
            override fun close() {
                logger.debug("received close on socket adapter")
                this@HttpRelayConnection.close()
            }

            @Throws(IOException::class)
            override fun getOutputStream(): OutputStream {
                return outputStream
            }

            @Throws(IOException::class)
            override fun getInputStream(): InputStream {
                return inputStream
            }

            override fun getRemoteSocketAddress(): SocketAddress {
                return InetSocketAddress(inetAddress, port)
            }

            override fun getPort(): Int {
                return 22067
            }

            override fun getInetAddress(): InetAddress {
                try {
                    return InetAddress.getByName(URI.create(this@HttpRelayConnection.httpRelayServerUrl).host)
                } catch (ex: UnknownHostException) {
                    throw RuntimeException(ex)
                }

            }

        }
    }

    private fun closeBg() {

        Thread { close() }.start()
    }

    private fun sendMessage(peerMessageBuilder: HttpRelayProtos.HttpRelayPeerMessage.Builder): HttpRelayProtos.HttpRelayServerMessage {
        try {
            if (!Strings.isNullOrEmpty(sessionId)) {
                peerMessageBuilder.sessionId = sessionId
            }
            logger.debug("send http relay peer message = {} session id = {} sequence = {}", peerMessageBuilder.messageType, peerMessageBuilder.sessionId, peerMessageBuilder.sequence)
            val httpClient = HttpClients.custom()
                    //                .setSSLSocketFactory(new SSLConnectionSocketFactory(new SSLContextBuilder().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build(), SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER))
                    .build()
            val httpPost = HttpPost(httpRelayServerUrl)
            httpPost.entity = ByteArrayEntity(peerMessageBuilder.build().toByteArray())
            val serverMessage = httpClient.execute(httpPost) { response ->
                checkArgument(equal(response.statusLine.statusCode, HttpStatus.SC_OK), "http error %s", response.statusLine)
                HttpRelayProtos.HttpRelayServerMessage.parseFrom(EntityUtils.toByteArray(response.entity))
            }
            logger.debug("received http relay server message = {}", serverMessage.getMessageType())
            checkArgument(!equal(serverMessage.getMessageType(), HttpRelayProtos.HttpRelayServerMessageType.ERROR), "server error : %s", object : Any() {
                override fun toString(): String {
                    return serverMessage.getData().toStringUtf8()
                }

            })
            return serverMessage
        } catch (ex: IOException) {
            throw RuntimeException(ex)
        }

    }

    override fun close() {
        if (!isClosed) {
            isClosed = true
            logger.info("closing http relay connection {} : {}", httpRelayServerUrl, sessionId)
            flusherStreamService.shutdown()
            if (!Strings.isNullOrEmpty(sessionId)) {
                try {
                    outputStream.flush()
                    sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder().setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.PEER_CLOSING))
                } catch (ex: IOException) {
                    logger.warn("error closing http relay connection", ex)
                }

            }
            incomingExecutorService.shutdown()
            outgoingExecutorService.shutdown()
            try {
                incomingExecutorService.awaitTermination(1, TimeUnit.SECONDS)
            } catch (ex: InterruptedException) {
                logger.warn("", ex)
            }

            try {
                outgoingExecutorService.awaitTermination(1, TimeUnit.SECONDS)
            } catch (ex: InterruptedException) {
                logger.warn("", ex)
            }

            try {
                flusherStreamService.awaitTermination(1, TimeUnit.SECONDS)
            } catch (ex: InterruptedException) {
                logger.warn("", ex)
            }

            incomingDataQueue.add(STREAM_CLOSED)
        }
    }

    companion object {
        private val STREAM_CLOSED = "STREAM_CLOSED".toByteArray()
    }
}
