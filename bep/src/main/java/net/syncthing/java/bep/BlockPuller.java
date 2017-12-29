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

import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import net.syncthing.java.core.beans.BlockInfo;
import net.syncthing.java.core.beans.FileBlocks;
import net.syncthing.java.core.cache.BlockCache;
import net.syncthing.java.core.configuration.ConfigurationService;
import net.syncthing.java.bep.BlockExchangeConnectionHandler.ResponseMessageReceivedEvent;
import net.syncthing.java.bep.BlockExchangeProtos.ErrorCode;
import net.syncthing.java.bep.BlockExchangeProtos.Request;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Preconditions.checkArgument;
import static net.syncthing.java.bep.BlockPusher.BLOCK_SIZE;

/**
 *
 * @author aleph
 */
public final class BlockPuller {

    private BlockCache blockCache;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final BlockExchangeConnectionHandler connectionHandler;
    private final Map<String, byte[]> blocksByHash = Maps.newConcurrentMap();
    private final List<String> hashList = Lists.newArrayList();
    private final Set<String> missingHashes = Sets.newConcurrentHashSet();
    private final Set<Integer> requestIds = Sets.newConcurrentHashSet();
    private boolean closeConnection = false;

    public BlockPuller(ConfigurationService configuration, BlockExchangeConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
        this.blockCache = BlockCache.getBlockCache(configuration);
    }

    public BlockPuller(ConfigurationService configuration, BlockExchangeConnectionHandler connectionHandler, boolean closeConnection) {
        this(configuration, connectionHandler);
        this.closeConnection = closeConnection;
    }

    public FileDownloadObserver pullBlocks(FileBlocks fileBlocks) {
        logger.info("pulling file = {}", fileBlocks);
        checkArgument(connectionHandler.hasFolder(fileBlocks.getFolder()), "supplied connection handler %s will not share folder %s", connectionHandler, fileBlocks.getFolder());
        final Object lock = new Object();
        final AtomicReference<Exception> error = new AtomicReference<>();
        final Object listener = new Object() {
            @Subscribe
            public void handleResponseMessageReceivedEvent(ResponseMessageReceivedEvent event) {
                synchronized (lock) {
                    if (!requestIds.contains(event.getMessage().getId())) {
                        return;
                    }
                    checkArgument(equal(event.getMessage().getCode(), ErrorCode.NO_ERROR), "received error response, code = %s", event.getMessage().getCode());
                    byte[] data = event.getMessage().getData().toByteArray();
                    String hash = BaseEncoding.base16().encode(Hashing.sha256().hashBytes(data).asBytes());
                    blockCache.pushBlock(data);
                    if (missingHashes.remove(hash)) {
                        blocksByHash.put(hash, data);
                        logger.debug("aquired block, hash = {}", hash);
                        lock.notify();
                    } else {
                        logger.warn("received not-needed block, hash = {}", hash);
                    }
                }
            }
        };
        FileDownloadObserver fileDownloadObserver = new FileDownloadObserver() {

            private long getReceivedData() {
                return blocksByHash.size() * BLOCK_SIZE;
            }

            private long getTotalData() {
                return (blocksByHash.size() + missingHashes.size()) * BLOCK_SIZE;
            }

            @Override
            public double getProgress() {
                return isCompleted() ? 1d : getReceivedData() / ((double) getTotalData());
            }

            @Override
            public String getProgressMessage() {
                return (Math.round(getProgress() * 1000d) / 10d) + "% " + FileUtils.byteCountToDisplaySize(getReceivedData()) + " / " + FileUtils.byteCountToDisplaySize(getTotalData());
            }

            @Override
            public boolean isCompleted() {
                return missingHashes.isEmpty();
            }

            @Override
            public void checkError() {
                if (error.get() != null) {
                    throw new RuntimeException(error.get());
                }
            }

            @Override
            public double waitForProgressUpdate() throws InterruptedException {
                if (!isCompleted()) {
                    synchronized (lock) {
                        checkError();
                        lock.wait();
                        checkError();
                    }
                }
                return getProgress();
            }

            @Override
            public InputStream getInputStream() {
                checkArgument(missingHashes.isEmpty(), "pull failed, some blocks are still missing");
                List<byte[]> blockList = Lists.newArrayList(Lists.transform(hashList, Functions.forMap(blocksByHash)));
                return new SequenceInputStream(Collections.enumeration(Lists.transform(blockList, ByteArrayInputStream::new)));
            }

            @Override
            public void close() {
                missingHashes.clear();
                hashList.clear();
                blocksByHash.clear();
                connectionHandler.getEventBus().unregister(listener);
                if (closeConnection) {
                    connectionHandler.close();
                }
            }
        };
        synchronized (lock) {
            hashList.addAll(Lists.transform(fileBlocks.getBlocks(), BlockInfo::getHash));
            missingHashes.addAll(hashList);
            for (String hash : missingHashes) {
                byte[] block = blockCache.pullBlock(hash);
                if (block != null) {
                    blocksByHash.put(hash, block);
                    missingHashes.remove(hash);
                }
            }
            connectionHandler.getEventBus().register(listener);
            for (BlockInfo block : fileBlocks.getBlocks()) {
                if (missingHashes.contains(block.getHash())) {
                    int requestId = Math.abs(new Random().nextInt());
                    requestIds.add(requestId);
                    connectionHandler.sendMessage(Request.newBuilder()
                        .setId(requestId)
                        .setFolder(fileBlocks.getFolder())
                        .setName(fileBlocks.getPath())
                        .setOffset(block.getOffset())
                        .setSize(block.getSize())
                        .setHash(ByteString.copyFrom(BaseEncoding.base16().decode(block.getHash())))
                        .build());
                    logger.debug("sent request for block, hash = {}", block.getHash());
                }
            }
            return fileDownloadObserver;
        }
    }

    public abstract class FileDownloadObserver implements Closeable {

        public abstract void checkError();

        public abstract double getProgress();

        public abstract String getProgressMessage();

        public abstract boolean isCompleted();

        public abstract double waitForProgressUpdate() throws InterruptedException;

        public FileDownloadObserver waitForComplete() throws InterruptedException {
            while (!isCompleted()) {
                waitForProgressUpdate();
            }
            return this;
        }

        public abstract InputStream getInputStream();

        @Override
        public abstract void close();

    }

}
