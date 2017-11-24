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
package it.anyplace.sync.httprelay.client;

import com.google.common.base.Strings;
import com.google.common.collect.Queues;
import com.google.protobuf.ByteString;

import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import it.anyplace.sync.core.interfaces.RelayConnection;
import it.anyplace.sync.httprelay.protos.HttpRelayProtos;

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 * @author aleph
 */
public class HttpRelayConnection implements RelayConnection, Closeable {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ExecutorService outgoingExecutorService = Executors.newSingleThreadExecutor(),
        incomingExecutorService = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService flusherStreamService = Executors.newSingleThreadScheduledExecutor();
    private long peerToRelaySequence = 0, relayToPeerSequence = 0;
    private String sessionId;
    private final BlockingQueue<byte[]> incomingDataQueue = Queues.newLinkedBlockingQueue();
    private final static byte[] STREAM_CLOSED = "STREAM_CLOSED".getBytes();
    private final String httpRelayServerUrl;
    private final Socket socket;
    private final boolean isServerSocket;
    private final InputStream inputStream;
    private final OutputStream outputStream;

    @Override
    public Socket getSocket() {
        return socket;
    }

    @Override
    public boolean isServerSocket() {
        return isServerSocket;
    }

    protected HttpRelayConnection(String httpRelayServerUrl, String deviceId) {
        this.httpRelayServerUrl = httpRelayServerUrl;
        HttpRelayProtos.HttpRelayServerMessage serverMessage = sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder()
            .setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.CONNECT)
            .setDeviceId(deviceId));
        checkArgument(equal(serverMessage.getMessageType(), HttpRelayProtos.HttpRelayServerMessageType.PEER_CONNECTED));
        checkNotNull(Strings.emptyToNull(serverMessage.getSessionId()));
        sessionId = serverMessage.getSessionId();
        isServerSocket = serverMessage.getIsServerSocket();
        outputStream = new OutputStream() {

            private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            private long lastFlush = System.currentTimeMillis();

            {
                flusherStreamService.scheduleWithFixedDelay(() -> {
                    if (System.currentTimeMillis() - lastFlush > 1000) {
                        try {
                            flush();
                        } catch (IOException ex) {
                            logger.warn("", ex);
                        }
                    }
                }, 1, 1, TimeUnit.SECONDS);
            }

            @Override
            public synchronized void write(int i) throws IOException {
                checkArgument(!HttpRelayConnection.this.isClosed());
                buffer.write(i);
            }

            @Override
            public synchronized void write(byte[] bytes, int offset, int size) throws IOException {
                checkArgument(!HttpRelayConnection.this.isClosed());
                buffer.write(bytes, offset, size);
            }

            @Override
            public synchronized void flush() throws IOException {
                final ByteString data = ByteString.copyFrom(buffer.toByteArray());
                buffer = new ByteArrayOutputStream();
                try {
                    if (!data.isEmpty()) {
                        outgoingExecutorService.submit(() -> {
                            sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder()
                                .setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.PEER_TO_RELAY)
                                .setSequence(++peerToRelaySequence)
                                .setData(data));
                        }).get();
                    }
                    lastFlush = System.currentTimeMillis();
                } catch (InterruptedException | ExecutionException ex) {
                    logger.error("error", ex);
                    closeBg();
                    throw new IOException(ex);
                }
            }

            @Override
            public synchronized void write(byte[] bytes) throws IOException {
                checkArgument(!HttpRelayConnection.this.isClosed());
                buffer.write(bytes);
            }

        };
        incomingExecutorService.submit(() -> {
            while (!isClosed()) {
                HttpRelayProtos.HttpRelayServerMessage serverMessage1 = sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder().setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.WAIT_FOR_DATA));
                if(isClosed()){
                    return;
                }
                checkArgument(equal(serverMessage1.getMessageType(), HttpRelayProtos.HttpRelayServerMessageType.RELAY_TO_PEER));
                checkArgument(serverMessage1.getSequence() == relayToPeerSequence + 1);
                if (!serverMessage1.getData().isEmpty()) {
                    incomingDataQueue.add(serverMessage1.getData().toByteArray());
                }
                relayToPeerSequence = serverMessage1.getSequence();
            }
        });
        inputStream = new InputStream() {

            private boolean noMoreData = false;
            private ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[0]);

            @Override
            public int read() throws IOException {
                checkArgument(!HttpRelayConnection.this.isClosed());
                if (noMoreData) {
                    return -1;
                }
                int bite;
                while ((bite = byteArrayInputStream.read()) == -1) {
                    try {
                        byte[] data = incomingDataQueue.poll(1, TimeUnit.SECONDS);
                        if (data == null) {
                            //continue
                        } else if (data == STREAM_CLOSED) {
                            noMoreData = true;
                            return -1;
                        } else {
                            byteArrayInputStream = new ByteArrayInputStream(data);
                        }
                    } catch (InterruptedException ex) {
                        logger.warn("", ex);
                    }
                }
                return bite;
            }

        };
        socket = new Socket() {
            @Override
            public boolean isClosed() {
                return HttpRelayConnection.this.isClosed();
            }

            @Override
            public boolean isConnected() {
                return !isClosed();
            }

            @Override
            public void shutdownOutput() throws IOException {
                logger.debug("shutdownOutput");
                getOutputStream().flush();
            }

            @Override
            public void shutdownInput() throws IOException {
                logger.debug("shutdownInput");
                //do nothing
            }

            @Override
            public synchronized void close() throws IOException {
                logger.debug("received close on socket adapter");
                HttpRelayConnection.this.close();
            }

            @Override
            public OutputStream getOutputStream() throws IOException {
                return outputStream;
            }

            @Override
            public InputStream getInputStream() throws IOException {
                return inputStream;
            }

            @Override
            public SocketAddress getRemoteSocketAddress() {
                return new InetSocketAddress(getInetAddress(), getPort());
            }

            @Override
            public int getPort() {
                return 22067;
            }

            @Override
            public InetAddress getInetAddress() {
                try {
                    return InetAddress.getByName(URI.create(HttpRelayConnection.this.httpRelayServerUrl).getHost());
                } catch (UnknownHostException ex) {
                    throw new RuntimeException(ex);
                }
            }

        };
    }

    private void closeBg() {

        new Thread(() -> close()).start();
    }

    private HttpRelayProtos.HttpRelayServerMessage sendMessage(HttpRelayProtos.HttpRelayPeerMessage.Builder peerMessageBuilder) {
        try {
            if (!Strings.isNullOrEmpty(sessionId)) {
                peerMessageBuilder.setSessionId(sessionId);
            }
            logger.debug("send http relay peer message = {} session id = {} sequence = {}", peerMessageBuilder.getMessageType(), peerMessageBuilder.getSessionId(), peerMessageBuilder.getSequence());
            HttpClient httpClient = HttpClients.custom()
                //                .setSSLSocketFactory(new SSLConnectionSocketFactory(new SSLContextBuilder().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build(), SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER))
                .build();
            HttpPost httpPost = new HttpPost(httpRelayServerUrl);
            httpPost.setEntity(new ByteArrayEntity(peerMessageBuilder.build().toByteArray()));
            final HttpRelayProtos.HttpRelayServerMessage serverMessage = httpClient.execute(httpPost, response -> {
                checkArgument(equal(response.getStatusLine().getStatusCode(), HttpStatus.SC_OK), "http error %s", response.getStatusLine());
                return HttpRelayProtos.HttpRelayServerMessage.parseFrom(EntityUtils.toByteArray(response.getEntity()));
            });
            logger.debug("received http relay server message = {}", serverMessage.getMessageType());
            checkArgument(!equal(serverMessage.getMessageType(), HttpRelayProtos.HttpRelayServerMessageType.ERROR), "server error : %s", new Object() {
                @Override
                public String toString() {
                    return serverMessage.getData().toStringUtf8();
                }

            });
            return serverMessage;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private boolean isClosed = false;

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public final void close() {
        if (!isClosed) {
            isClosed = true;
            logger.info("closing http relay connection {} : {}", httpRelayServerUrl, sessionId);
            flusherStreamService.shutdown();
            if (!Strings.isNullOrEmpty(sessionId)) {
                try {
                    outputStream.flush();
                    sendMessage(HttpRelayProtos.HttpRelayPeerMessage.newBuilder().setMessageType(HttpRelayProtos.HttpRelayPeerMessageType.PEER_CLOSING));
                } catch (IOException ex) {
                    logger.warn("error closing http relay connection", ex);
                }
            }
            incomingExecutorService.shutdown();
            outgoingExecutorService.shutdown();
            try {
                incomingExecutorService.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                logger.warn("", ex);
            }
            try {
                outgoingExecutorService.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                logger.warn("", ex);
            }
            try {
                flusherStreamService.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                logger.warn("", ex);
            }
            incomingDataQueue.add(STREAM_CLOSED);
        }
    }
}
