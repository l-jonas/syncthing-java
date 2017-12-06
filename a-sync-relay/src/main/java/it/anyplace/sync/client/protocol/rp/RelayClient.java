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
package it.anyplace.sync.client.protocol.rp;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import it.anyplace.sync.client.protocol.rp.beans.SessionInvitation;
import it.anyplace.sync.core.beans.DeviceAddress;
import it.anyplace.sync.core.beans.DeviceAddress.AddressType;
import it.anyplace.sync.core.configuration.ConfigurationService;
import it.anyplace.sync.core.interfaces.RelayConnection;
import it.anyplace.sync.core.security.KeystoreHandler;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static it.anyplace.sync.core.security.KeystoreHandler.*;

/**
 *
 * @author aleph
 */
public final class RelayClient {

    private final static int MAGIC = 0x9E79BC40,
        JOIN_SESSION_REQUEST = 3,
        RESPONSE = 4,
        CONNECT_REQUEST = 5,
        SESSION_INVITATION = 6,
        RESPONSE_SUCCESS_CODE = 0;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final KeystoreHandler keystoreHandler;

    public RelayClient(ConfigurationService configuration) {
        this.keystoreHandler = KeystoreHandler.newLoader().loadAndStore(configuration);
    }

    public RelayConnection openRelayConnection(DeviceAddress address) throws IOException, KeystoreHandler.CryptoException {
        Preconditions.checkArgument(address.getType().equals(AddressType.RELAY));
        SessionInvitation sessionInvitation = getSessionInvitation(address.getSocketAddress(), address.getDeviceId());
        return openConnectionSessionMode(sessionInvitation);
    }

    public RelayConnection openConnectionSessionMode(final SessionInvitation sessionInvitation) throws IOException {
        logger.debug("connecting to relay = {}:{} (session mode)", sessionInvitation.getAddress(), sessionInvitation.getPort());
        final Socket socket = new Socket(sessionInvitation.getAddress(), sessionInvitation.getPort());
        RelayDataInputStream in = new RelayDataInputStream(socket.getInputStream());
        RelayDataOutputStream out = new RelayDataOutputStream(socket.getOutputStream());
        {
            logger.debug("sending join session request, session key = {}", sessionInvitation.getKey());
            byte[] key = BaseEncoding.base16().decode(sessionInvitation.getKey());
            int lengthOfKey = key.length;
            out.writeHeader(JOIN_SESSION_REQUEST, 4 + lengthOfKey);
            out.writeInt(lengthOfKey);
            out.write(key);
            out.flush();
        }
        {
            logger.debug("reading relay response");
            MessageReader messageReader = in.readMessage();
            checkArgument(messageReader.getType() == RESPONSE);
            Response response = messageReader.readResponse();
            logger.debug("response = {}", response);
            checkArgument(response.getCode() == RESPONSE_SUCCESS_CODE, "response code = %s (%s) expected %s", response.getCode(), response.getMessage(), RESPONSE_SUCCESS_CODE);
            logger.debug("relay connection ready");
        }
        return new RelayConnection() {
            @Override
            public Socket getSocket() {
                return socket;
            }

            @Override
            public boolean isServerSocket() {
                return sessionInvitation.isServerSocket();
            }

        };
    }

    public SessionInvitation getSessionInvitation(InetSocketAddress relaySocketAddress, String deviceId) throws IOException, KeystoreHandler.CryptoException {
        logger.debug("connecting to relay = {} (temporary protocol mode)", relaySocketAddress);
        try (Socket socket = keystoreHandler.createSocket(relaySocketAddress, RELAY);
            RelayDataInputStream in = new RelayDataInputStream(socket.getInputStream());
            RelayDataOutputStream out = new RelayDataOutputStream(socket.getOutputStream())) {
            {
                logger.debug("sending connect request for device = {}", deviceId);
                byte[] deviceIdData = deviceIdStringToHashData(deviceId);
                int lengthOfId = deviceIdData.length;
                out.writeHeader(CONNECT_REQUEST, 4 + lengthOfId);
                out.writeInt(lengthOfId);
                out.write(deviceIdData);
                out.flush();
            }

            {
                logger.debug("receiving session invitation");
                MessageReader messageReader = in.readMessage();
                logger.debug("received message = {}", messageReader.dumpMessageForDebug());
                checkArgument(messageReader.getType() == SESSION_INVITATION, "message type mismatch, expected %s, got %s", SESSION_INVITATION, messageReader.getType());
                SessionInvitation.Builder builder = SessionInvitation.newBuilder();
                builder.setFrom(hashDataToDeviceIdString(messageReader.readLengthAndData()));
                builder.setKey(BaseEncoding.base16().encode(messageReader.readLengthAndData()));
                byte[] address = messageReader.readLengthAndData();
                if (address.length == 0) {
                    builder.setAddress(socket.getInetAddress());
                } else {
                    InetAddress inetAddress = InetAddress.getByAddress(address);
                    if (inetAddress.equals(InetAddress.getByName("0.0.0.0"))) {
                        builder.setAddress(socket.getInetAddress());
                    } else {
                        builder.setAddress(inetAddress);
                    }
                }
                int zero = messageReader.getBuffer().getShort();
                checkArgument(zero == 0, "expected 0, found %s", zero);
                int port = messageReader.getBuffer().getShort();
                checkArgument(port > 0, "got invalid port value = %s", port);
                builder.setPort(port);
                int serverSocket = messageReader.getBuffer().getInt() & 1;
                builder.setServerSocket(serverSocket == 1);
                logger.debug("closing connection (temporary protocol mode)");
                return builder.build();
            }
        }
    }

    private static final class RelayDataOutputStream extends DataOutputStream {

        public RelayDataOutputStream(OutputStream out) {
            super(out);
        }

        private void writeHeader(int type, int length) throws IOException {
            writeInt(MAGIC);
            writeInt(type);
            writeInt(length);
        }

    }

    private static final class RelayDataInputStream extends DataInputStream {

        public RelayDataInputStream(InputStream in) {
            super(in);
        }

        public MessageReader readMessage() throws IOException {
            int magic = readInt();
            checkArgument(magic == MAGIC, "magic mismatch, got = %s, expected = %s", magic, MAGIC);
            int type = readInt();
            int length = readInt();
            checkArgument(length >= 0);
            ByteBuffer payload = ByteBuffer.allocate(length);
            IOUtils.readFully(this, payload.array());
            return new MessageReader(type, payload);
        }
    }

    private static final class Response {

        private final int code;
        private final String message;

        public Response(int code, String message) {
            this.code = code;
            this.message = message;
        }

        public int getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return "Response{" + "code=" + code + ", message=" + message + '}';
        }

    }

    private static final class MessageReader {

        private final int type;
        private final ByteBuffer buffer;

        public MessageReader(int type, ByteBuffer buffer) {
            this.type = type;
            this.buffer = buffer;
        }

        public int getType() {
            return type;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public byte[] readLengthAndData() {
            int length = buffer.getInt();
            checkArgument(length >= 0);
            byte[] data = new byte[length];
            buffer.get(data);
            return data;
        }

        public Response readResponse() {
            int code = buffer.getInt();
            int messageLength = buffer.getInt();
            byte[] message = new byte[messageLength];
            buffer.get(message);
            return new Response(code, new String(message));
        }

        public MessageReader cloneReader() {
            return new MessageReader(type, ByteBuffer.wrap(buffer.array()));
        }

        private String dumpMessageForDebug() {
            if (type == RESPONSE) {
                return MoreObjects.toStringHelper("Response").add("code", cloneReader().readResponse().getCode()).add("message", cloneReader().readResponse().getMessage()).toString();
            } else {
                return MoreObjects.toStringHelper("Message").add("type", type).add("size", buffer.capacity()).toString();
            }
        }
    }

}
