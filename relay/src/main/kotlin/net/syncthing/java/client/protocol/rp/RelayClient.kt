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
package net.syncthing.java.client.protocol.rp

import com.google.common.base.MoreObjects
import com.google.common.base.Preconditions
import com.google.common.io.BaseEncoding
import net.syncthing.java.client.protocol.rp.beans.SessionInvitation
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceAddress.AddressType
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.interfaces.RelayConnection
import net.syncthing.java.core.security.KeystoreHandler
import org.apache.commons.io.IOUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.*
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.ByteBuffer

import com.google.common.base.Preconditions.checkArgument
import net.syncthing.java.core.security.KeystoreHandler.*

class RelayClient(configuration: ConfigurationService) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val keystoreHandler: KeystoreHandler

    init {
        this.keystoreHandler = KeystoreHandler.Loader().loadAndStore(configuration)
    }

    @Throws(IOException::class, KeystoreHandler.CryptoException::class)
    fun openRelayConnection(address: DeviceAddress): RelayConnection {
        Preconditions.checkArgument(address.type == AddressType.RELAY)
        val sessionInvitation = getSessionInvitation(address.socketAddress, address.deviceId)
        return openConnectionSessionMode(sessionInvitation)
    }

    @Throws(IOException::class)
    fun openConnectionSessionMode(sessionInvitation: SessionInvitation): RelayConnection {
        logger.debug("connecting to relay = {}:{} (session mode)", sessionInvitation.address, sessionInvitation.port)
        val socket = Socket(sessionInvitation.address, sessionInvitation.port)
        val `in` = RelayDataInputStream(socket.getInputStream())
        val out = RelayDataOutputStream(socket.getOutputStream())
        run {
            logger.debug("sending join session request, session key = {}", sessionInvitation.key)
            val key = BaseEncoding.base16().decode(sessionInvitation.key)
            val lengthOfKey = key.size
            out.writeHeader(JOIN_SESSION_REQUEST, 4 + lengthOfKey)
            out.writeInt(lengthOfKey)
            out.write(key)
            out.flush()
        }
        run {
            logger.debug("reading relay response")
            val messageReader = `in`.readMessage()
            checkArgument(messageReader.type == RESPONSE)
            val response = messageReader.readResponse()
            logger.debug("response = {}", response)
            checkArgument(response.code == ResponseSuccess, "response code = %s (%s) expected %s", response.code, response.message, ResponseSuccess)
            logger.debug("relay connection ready")
        }
        return object : RelayConnection {
            override fun getSocket(): Socket {
                return socket
            }

            override fun isServerSocket(): Boolean {
                return sessionInvitation.isServerSocket
            }

        }
    }

    @Throws(IOException::class, KeystoreHandler.CryptoException::class)
    fun getSessionInvitation(relaySocketAddress: InetSocketAddress, deviceId: String): SessionInvitation {
        logger.debug("connecting to relay = {} (temporary protocol mode)", relaySocketAddress)
        keystoreHandler.createSocket(relaySocketAddress, KeystoreHandler.RELAY).use { socket ->
            RelayDataInputStream(socket.getInputStream()).use { `in` ->
                RelayDataOutputStream(socket.getOutputStream()).use { out ->
                    run {
                        logger.debug("sending connect request for device = {}", deviceId)
                        val deviceIdData = KeystoreHandler.deviceIdStringToHashData(deviceId)
                        val lengthOfId = deviceIdData.size
                        out.writeHeader(CONNECT_REQUEST, 4 + lengthOfId)
                        out.writeInt(lengthOfId)
                        out.write(deviceIdData)
                        out.flush()
                    }

                    run {
                        logger.debug("receiving session invitation")
                        val messageReader = `in`.readMessage()
                        logger.debug("received message = {}", messageReader.dumpMessageForDebug())
                        if (messageReader.type == RESPONSE) {
                            val response = messageReader.readResponse()
                            throw IOException(response.message)
                        }
                        checkArgument(messageReader.type == SESSION_INVITATION, "message type mismatch, expected %s, got %s", SESSION_INVITATION, messageReader.type)
                        val builder = SessionInvitation.Builder()
                                .setFrom(KeystoreHandler.hashDataToDeviceIdString(messageReader.readLengthAndData()))
                                .setKey(BaseEncoding.base16().encode(messageReader.readLengthAndData()))
                            val address = messageReader.readLengthAndData()
                        if (address.size == 0) {
                            builder.setAddress(socket.inetAddress)
                        } else {
                            val inetAddress = InetAddress.getByAddress(address)
                            if (inetAddress == InetAddress.getByName("0.0.0.0")) {
                                builder.setAddress(socket.inetAddress)
                            } else {
                                builder.setAddress(inetAddress)
                            }
                        }
                        val zero = messageReader.buffer.short.toInt()
                        checkArgument(zero == 0, "expected 0, found %s", zero)
                        val port = messageReader.buffer.short.toInt()
                        checkArgument(port > 0, "got invalid port value = %s", port)
                        builder.setPort(port)
                        val serverSocket = messageReader.buffer.int and 1
                        builder.setServerSocket(serverSocket == 1)
                        logger.debug("closing connection (temporary protocol mode)")
                        return builder.build()
                    }
                }
            }
        }
    }

    private class RelayDataOutputStream(out: OutputStream) : DataOutputStream(out) {

        @Throws(IOException::class)
        fun writeHeader(type: Int, length: Int) {
            writeInt(MAGIC)
            writeInt(type)
            writeInt(length)
        }

    }

    private class RelayDataInputStream(`in`: InputStream) : DataInputStream(`in`) {

        @Throws(IOException::class)
        fun readMessage(): MessageReader {
            val magic = readInt()
            checkArgument(magic == MAGIC, "magic mismatch, got = %s, expected = %s", magic, MAGIC)
            val type = readInt()
            val length = readInt()
            checkArgument(length >= 0)
            val payload = ByteBuffer.allocate(length)
            IOUtils.readFully(this, payload.array())
            return MessageReader(type, payload)
        }
    }

    private class Response(val code: Int, val message: String) {

        override fun toString(): String {
            return "Response{code=$code, message=$message}"
        }

    }

    private class MessageReader(val type: Int, val buffer: ByteBuffer) {

        fun readLengthAndData(): ByteArray {
            val length = buffer.int
            checkArgument(length >= 0)
            val data = ByteArray(length)
            buffer.get(data)
            return data
        }

        fun readResponse(): Response {
            val code = buffer.int
            val messageLength = buffer.int
            val message = ByteArray(messageLength)
            buffer.get(message)
            return Response(code, String(message))
        }

        fun cloneReader(): MessageReader {
            return MessageReader(type, ByteBuffer.wrap(buffer.array()))
        }

        fun dumpMessageForDebug(): String {
            return if (type == RESPONSE) {
                MoreObjects.toStringHelper("Response").add("code", cloneReader().readResponse().code).add("message", cloneReader().readResponse().message).toString()
            } else {
                MoreObjects.toStringHelper("Message").add("type", type).add("size", buffer.capacity()).toString()
            }
        }
    }

    companion object {

        private val MAGIC = -0x618643c0
        private val JOIN_SESSION_REQUEST = 3
        private val RESPONSE = 4
        private val CONNECT_REQUEST = 5
        private val SESSION_INVITATION = 6
        private val ResponseSuccess = 0
        private val ResponseNotFound = 1
        private val ResponseAlreadyConnected = 2
    }

}
