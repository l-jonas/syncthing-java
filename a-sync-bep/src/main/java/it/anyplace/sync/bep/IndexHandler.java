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
package it.anyplace.sync.bep;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.BaseEncoding;
import it.anyplace.sync.bep.BlockExchangeConnectionHandler.AnyIndexMessageReceivedEvent;
import it.anyplace.sync.bep.BlockExchangeConnectionHandler.ClusterConfigInfo;
import it.anyplace.sync.bep.BlockExchangeConnectionHandler.ClusterConfigMessageProcessedEvent;
import it.anyplace.sync.core.beans.*;
import it.anyplace.sync.core.beans.FileInfo.Version;
import it.anyplace.sync.core.configuration.ConfigurationService;
import it.anyplace.sync.core.interfaces.IndexRepository;
import it.anyplace.sync.core.interfaces.Sequencer;
import it.anyplace.sync.core.interfaces.TempRepository;
import it.anyplace.sync.core.utils.ExecutorUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static it.anyplace.sync.core.security.KeystoreHandler.hashDataToDeviceIdString;

/**
 *
 * @author aleph
 */
public final class IndexHandler implements Closeable {

    private final static long DEFAULT_INDEX_TIMEOUT = 30;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ConfigurationService configuration;
    private final EventBus eventBus = new EventBus();
    private final Map<String, FolderInfo> folderInfoByFolder = Maps.newHashMap();
    private final IndexRepository indexRepository;
    private final TempRepository tempRepository;
    private final IndexMessageProcessor indexMessageProcessor = new IndexMessageProcessor();
    private long lastIndexActivity = 0;
    private final Object writeAccessLock = new Object(), indexWaitLock = new Object();

    private long getLastActive() {
        return System.currentTimeMillis() - lastIndexActivity;
    }

    private void markActive() {
        lastIndexActivity = System.currentTimeMillis();
    }

    public IndexHandler(ConfigurationService configuration, IndexRepository indexRepository, TempRepository tempRepository) {
        this.configuration = configuration;
        loadFolderInfoFromConfig();
        this.indexRepository = indexRepository;
        this.tempRepository = tempRepository;
    }

    private void loadFolderInfoFromConfig() {
        synchronized (writeAccessLock) {
            for (FolderInfo folderInfo : configuration.getFolders()) {
                folderInfoByFolder.put(folderInfo.getFolder(), folderInfo); //TODO reference 'folder info' repository
            }
        }
    }

    public EventBus getEventBus() {
        return eventBus;
    }

    public synchronized void clearIndex() {
        synchronized (writeAccessLock) {
            indexRepository.clearIndex();
            folderInfoByFolder.clear();
            loadFolderInfoFromConfig();
        }
    }

    public boolean isRemoteIndexAquired(ClusterConfigInfo clusterConfigInfo, String peerDeviceId) {
        boolean ready = true;
        for (String folder : clusterConfigInfo.getSharedFolders()) {
            IndexInfo indexSequenceInfo = indexRepository.findIndexInfoByDeviceAndFolder(peerDeviceId, folder);
            if (indexSequenceInfo == null || indexSequenceInfo.getLocalSequence() < indexSequenceInfo.getMaxSequence()) {
                logger.debug("waiting for index on folder = {} sequenceInfo = {}", folder, indexSequenceInfo);
                ready = false;
            }
        }
        return ready;
    }

    public IndexHandler waitForRemoteIndexAquired(BlockExchangeConnectionHandler connectionHandler) throws InterruptedException {
        return waitForRemoteIndexAquired(connectionHandler, null);
    }

    public IndexHandler waitForRemoteIndexAquired(BlockExchangeConnectionHandler connectionHandler, @Nullable Long timeoutSecs) throws InterruptedException {
        long timeoutMillis = firstNonNull(timeoutSecs, DEFAULT_INDEX_TIMEOUT) * 1000;
        synchronized (indexWaitLock) {
            while (!isRemoteIndexAquired(connectionHandler.getClusterConfigInfo(), connectionHandler.getDeviceId())) {
                indexWaitLock.wait(timeoutMillis);
                checkArgument(connectionHandler.getLastActive() < timeoutMillis
                    || getLastActive() < timeoutMillis, "unable to aquire index from connection %s, timeout reached!", connectionHandler);
            }
        }
        logger.debug("aquired all indexes on connection {}", connectionHandler);
        return this;
    }

    @Subscribe
    public void handleClusterConfigMessageProcessedEvent(ClusterConfigMessageProcessedEvent event) {
        synchronized (writeAccessLock) {
            for (BlockExchageProtos.Folder folderRecord : event.getMessage().getFoldersList()) {
                String folder = folderRecord.getId();
                FolderInfo folderInfo = updateFolderInfo(folder, folderRecord.getLabel());
                logger.debug("aquired folder info from cluster config = {}", folderInfo);
                for (BlockExchageProtos.Device deviceRecord : folderRecord.getDevicesList()) {
                    String deviceId = hashDataToDeviceIdString(deviceRecord.getId().toByteArray());
                    if (deviceRecord.hasIndexId() && deviceRecord.hasMaxSequence()) {
                        IndexInfo folderIndexInfo = updateIndexInfo(folder, deviceId, deviceRecord.getIndexId(), deviceRecord.getMaxSequence(), null);
                        logger.debug("aquired folder index info from cluster config = {}", folderIndexInfo);
                    }
                }
            }
        }
    }

    @Subscribe
    public void handleIndexMessageReceivedEvent(AnyIndexMessageReceivedEvent event) {
        indexMessageProcessor.handleIndexMessageReceivedEvent(event);
    }

    public @Nullable
    FileInfo pushRecord(String folder, BlockExchageProtos.FileInfo bepFileInfo) {
        FileBlocks fileBlocks = null;
        FileInfo.Builder builder = FileInfo.newBuilder()
            .setFolder(folder)
            .setPath(bepFileInfo.getName())
            .setLastModified(new Date(bepFileInfo.getModifiedS() * 1000 + bepFileInfo.getModifiedNs() / 1000000))
            .setVersionList(Iterables.transform(firstNonNull(bepFileInfo.hasVersion() ? bepFileInfo.getVersion().getCountersList() : null, Collections.emptyList()), record -> new Version(record.getId(), record.getValue())))
            .setDeleted(bepFileInfo.getDeleted());
        switch (bepFileInfo.getType()) {
            case FILE:
                fileBlocks = new FileBlocks(folder, builder.getPath(), Iterables.transform(firstNonNull(bepFileInfo.getBlocksList(), Collections.emptyList()), record -> new BlockInfo(record.getOffset(), record.getSize(), BaseEncoding.base16().encode(record.getHash().toByteArray()))));
                builder
                    .setTypeFile()
                    .setHash(fileBlocks.getHash())
                    .setSize(bepFileInfo.getSize());
                break;
            case DIRECTORY:
                builder.setTypeDir();
                break;
            default:
                logger.warn("unsupported file type = {}, discarding file info", bepFileInfo.getType());
                return null;
        }
        return addRecord(builder.build(), fileBlocks);
    }

    private IndexInfo updateIndexInfo(final String folder, final String deviceId, @Nullable Long indexId, @Nullable Long maxSequence, @Nullable Long localSequence) {
        synchronized (writeAccessLock) {
            IndexInfo indexSequenceInfo = indexRepository.findIndexInfoByDeviceAndFolder(deviceId, folder);
            boolean shouldUpdate = false;
            IndexInfo.Builder builder;
            if (indexSequenceInfo == null) {
                shouldUpdate = true;
                checkNotNull(indexId, "index sequence info not found, and supplied null index id (folder = %s, device = %s)", folder, deviceId);
                builder = IndexInfo.newBuilder()
                    .setFolder(folder)
                    .setDeviceId(deviceId)
                    .setIndexId(indexId)
                    .setLocalSequence(0)
                    .setMaxSequence(-1);
            } else {
                builder = indexSequenceInfo.copyBuilder();
            }
            if (indexId != null && indexId != builder.getIndexId()) {
                shouldUpdate = true;
                builder.setIndexId(indexId);
            }
            if (maxSequence != null && maxSequence > builder.getMaxSequence()) {
                shouldUpdate = true;
                builder.setMaxSequence(maxSequence);
            }
            if (localSequence != null && localSequence > builder.getLocalSequence()) {
                shouldUpdate = true;
                builder.setLocalSequence(localSequence);
            }
            if (shouldUpdate) {
                indexSequenceInfo = builder.build();
                indexRepository.updateIndexInfo(indexSequenceInfo);
            }
            return indexSequenceInfo;
        }
    }

    private @Nullable
    FileInfo addRecord(final FileInfo record, @Nullable final FileBlocks fileBlocks) {
        synchronized (writeAccessLock) {
            Date lastModified = indexRepository.findFileInfoLastModified(record.getFolder(), record.getPath());
            if (lastModified != null && !record.getLastModified().after(lastModified)) {
                logger.trace("discarding record = {}, modified before local record", record);
                return null;
            } else {
                indexRepository.updateFileInfo(record, fileBlocks);
                logger.trace("loaded new record = {}", record);
                eventBus.post(new IndexChangedEvent() {
                    @Override
                    public String getFolder() {
                        return record.getFolder();
                    }

                    @Override
                    public List<FileInfo> getNewRecords() {
                        return Collections.singletonList(record);
                    }
                });
                return record;
            }
        }
    }

    public IndexBrowser.Builder newIndexBrowserBuilder() {
        return IndexBrowser.newBuilder().setIndexHandler(this).setIndexRepository(indexRepository);
    }

    public IndexFinder.Builder newIndexFinderBuilder() {
        return IndexFinder.newBuilder().setIndexRepository(indexRepository);
    }

    public @Nullable
    FileInfo getFileInfoByPath(String folder, String path) {
        return indexRepository.findFileInfo(folder, path);
    }

    public @Nullable
    Pair<FileInfo, FileBlocks> getFileInfoAndBlocksByPath(String folder, String path) {
        FileInfo fileInfo = getFileInfoByPath(folder, path);
        if (fileInfo == null) {
            return null;
        } else {
            checkArgument(fileInfo.isFile());
            FileBlocks fileBlocks = indexRepository.findFileBlocks(folder, path);
            checkNotNull(fileBlocks, "file blocks not found for file info = %s", fileInfo);
            return Pair.of(fileInfo, fileBlocks);
        }
    }

    private FolderInfo updateFolderInfo(String folder, @Nullable String label) {
        FolderInfo folderInfo = folderInfoByFolder.get(folder);
        if (folderInfo == null || !Strings.isNullOrEmpty(label)) {
            folderInfo = new FolderInfo(folder, label);
            folderInfoByFolder.put(folderInfo.getFolder(), folderInfo);
//            configuration.addFolders(folderInfo); //update folders in config; not needed, added by connectionHandler
        }
        return folderInfo;
    }

    public FolderInfo getFolderInfo(String folder) {
        return folderInfoByFolder.get(folder);
    }

    public IndexInfo getIndexInfo(String device, String folder) {
        return indexRepository.findIndexInfoByDeviceAndFolder(device, folder);
    }

    public IndexRepository getIndexRepository() {
        return indexRepository;
    }

    public Sequencer getSequencer() {
        return indexRepository.getSequencer();
    }

    public List<String> getFolderList() {
        return Lists.newArrayList(folderInfoByFolder.keySet());
    }

    public List<FolderInfo> getFolderInfoList() {
        return Lists.newArrayList(folderInfoByFolder.values());
    }

    public FolderBrowser newFolderBrowser() {
        return new FolderBrowser(this);
    }

    @Override
    public void close() {
        indexMessageProcessor.stop();
    }

    public abstract class IndexRecordAquiredEvent {

        private Set<String> affectedPaths;

        public abstract String getFolder();

        public abstract List<FileInfo> getNewRecords();

        public abstract IndexInfo getIndexInfo();

        public synchronized Set<String> getAffectedPaths() {
            if (affectedPaths == null) {
                affectedPaths = Sets.newHashSet();
                for (FileInfo fileInfo : getNewRecords()) {
                    affectedPaths.add(fileInfo.getPath());
                    affectedPaths.add(fileInfo.getParent());
                }
            }
            return affectedPaths;
        }

    }

    public abstract class IndexChangedEvent {

        public abstract String getFolder();

        public abstract List<FileInfo> getNewRecords();

    }

    public abstract class FullIndexAquiredEvent {

        public abstract String getFolder();

    }

    private final class IndexMessageProcessor {

        private final Logger logger = LoggerFactory.getLogger(getClass());
        private final ExecutorService executorService = Executors.newSingleThreadExecutor();
        private int queuedMessages = 0;
        private long queuedRecords = 0;
//        private long lastRecordProcessingTime = 0;
//        , delay = 0;
//        private boolean addProcessingDelayForInterface = true;
//        private final int MIN_DELAY = 0, MAX_DELAY = 5000, MAX_RECORD_PER_PROCESS = 16, DELAY_FACTOR = 1;
        private Stopwatch stopwatch;

        private void handleIndexMessageReceivedEvent(AnyIndexMessageReceivedEvent event) {
            logger.info("received index message event, preparing (queued records = {} event record count = {})", queuedRecords, event.getFilesList().size());
            markActive();
            ClusterConfigInfo clusterConfigInfo = event.getConnectionHandler().getClusterConfigInfo();
            String peerDeviceId = event.getConnectionHandler().getDeviceId();
//            List<BlockExchageProtos.FileInfo> fileList = event.getFilesList();
//            for (int index = 0; index < fileList.size(); index += MAX_RECORD_PER_PROCESS) {
//                BlockExchageProtos.IndexUpdate data = BlockExchageProtos.IndexUpdate.newBuilder()
//                    .addAllFiles(Iterables.limit(Iterables.skip(fileList, index), MAX_RECORD_PER_PROCESS))
//                    .setFolder(event.getFolder())
//                    .build();
//                if (queuedMessages > 0) {
//                    storeAndProcessBg(data, clusterConfigInfo, peerDeviceId);
//                } else {
//                    processBg(data, clusterConfigInfo, peerDeviceId);
//                }
//            }
            BlockExchageProtos.IndexUpdate data = BlockExchageProtos.IndexUpdate.newBuilder()
                .addAllFiles(event.getFilesList())
                .setFolder(event.getFolder())
                .build();
            if (queuedMessages > 0) {
                storeAndProcessBg(data, clusterConfigInfo, peerDeviceId);
            } else {
                processBg(data, clusterConfigInfo, peerDeviceId);
            }
        }

        private void processBg(final BlockExchageProtos.IndexUpdate data, final ClusterConfigInfo clusterConfigInfo, final String peerDeviceId) {
            logger.debug("received index message event, queuing for processing");
            queuedMessages++;
            queuedRecords += data.getFilesCount();
            executorService.submit(new ProcessingRunnable() {
                @Override
                protected void runProcess() {
                    doHandleIndexMessageReceivedEvent(data, clusterConfigInfo, peerDeviceId);
                }

            });
        }

        private void storeAndProcessBg(final BlockExchageProtos.IndexUpdate data, final ClusterConfigInfo clusterConfigInfo, final String peerDeviceId) {
            final String key = tempRepository.pushTempData(data.toByteArray());
            logger.debug("received index message event, stored to temp record {}, queuing for processing", key);
            queuedMessages++;
            queuedRecords += data.getFilesCount();
            executorService.submit(new ProcessingRunnable() {
                @Override
                protected void runProcess() {
                    try {
                        doHandleIndexMessageReceivedEvent(key, clusterConfigInfo, peerDeviceId);
                    } catch (IOException ex) {
                        logger.error("error processing index message", ex);
                    }
                }

            });
        }

        private abstract class ProcessingRunnable implements Runnable {

            @Override
            public void run() {
                stopwatch = Stopwatch.createStarted();
                runProcess();
                queuedMessages--;
//                lastRecordProcessingTime = stopwatch.elapsed(TimeUnit.MILLISECONDS) - delay;
//                logger.info("processed a bunch of records, {}*{} remaining", queuedMessages, MAX_RECORD_PER_PROCESS);
//                logger.debug("processed index message in {} secs", lastRecordProcessingTime / 1000d);
                stopwatch = null;
            }

            protected abstract void runProcess();

//        private boolean isVersionOlderThanSequence(BlockExchageProtos.FileInfo fileInfo, long localSequence) {
//            long fileSequence = fileInfo.getSequence();
//            //TODO should we check last version instead of sequence? verify
//            return fileSequence < localSequence;
//        }
            protected void doHandleIndexMessageReceivedEvent(String key, ClusterConfigInfo clusterConfigInfo, String peerDeviceId) throws IOException {
                logger.debug("processing index message event from temp record {}", key);
                markActive();
                byte[] data = tempRepository.popTempData(key);
                BlockExchageProtos.IndexUpdate message = BlockExchageProtos.IndexUpdate.parseFrom(data);
                doHandleIndexMessageReceivedEvent(message, clusterConfigInfo, peerDeviceId);
            }

            protected void doHandleIndexMessageReceivedEvent(BlockExchageProtos.IndexUpdate message, ClusterConfigInfo clusterConfigInfo, String peerDeviceId) {
//            synchronized (writeAccessLock) {
//                if (addProcessingDelayForInterface) {
//                    delay = Math.min(MAX_DELAY, Math.max(MIN_DELAY, lastRecordProcessingTime * DELAY_FACTOR));
//                    logger.info("add delay of {} secs before processing index message (to allow UI to process)", delay / 1000d);
//                    try {
//                        Thread.sleep(delay);
//                    } catch (InterruptedException ex) {
//                        logger.warn("interrupted", ex);
//                    }
//                } else {
//                    delay = 0;
//                }
                logger.info("processing index message with {} records (queue size: messages = {} records = {})", message.getFilesCount(), queuedMessages, queuedRecords);
//            String deviceId = connectionHandler.getDeviceId();
                final String folder = message.getFolder();
                long sequence = -1;
                final List<FileInfo> newRecords = Lists.newArrayList();
//                IndexInfo oldIndexInfo = indexRepository.findIndexInfoByDeviceAndFolder(deviceId, folder);
//            Stopwatch stopwatch = Stopwatch.createStarted();
                logger.debug("processing {} index records for folder {}", message.getFilesList().size(), folder);
                for (BlockExchageProtos.FileInfo fileInfo : message.getFilesList()) {
                    markActive();
//                    if (oldIndexInfo != null && isVersionOlderThanSequence(fileInfo, oldIndexInfo.getLocalSequence())) {
//                        logger.trace("skipping file {}, version older than sequence {}", fileInfo, oldIndexInfo.getLocalSequence());
//                    } else {
                    FileInfo newRecord = pushRecord(folder, fileInfo);
                    if (newRecord != null) {
                        newRecords.add(newRecord);
                    }
                    sequence = Math.max(fileInfo.getSequence(), sequence);
                    markActive();
//                    }
                }
                final IndexInfo newIndexInfo = updateIndexInfo(folder, peerDeviceId, null, null, sequence);
                long elap = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                queuedRecords -= message.getFilesCount();
                logger.info("processed {} index records, aquired {} ({} secs, {} record/sec)", message.getFilesCount(), newRecords.size(), elap / 1000d, Math.round((message.getFilesCount() / (elap / 1000d)) * 100) / 100d);
                logger.info("remaining queue size: messages = {} records = {}; eta {} min", queuedMessages, queuedRecords, Math.round(queuedRecords / message.getFilesCount() * (elap / 1000d)) / 60d);
                if (logger.isInfoEnabled() && newRecords.size() <= 10) {
                    for (FileInfo fileInfo : newRecords) {
                        logger.info("aquired record = {}", fileInfo);
                    }
                }
                if (!newRecords.isEmpty()) {
                    eventBus.post(new IndexRecordAquiredEvent() {
                        @Override
                        public String getFolder() {
                            return folder;
                        }

                        @Override
                        public List<FileInfo> getNewRecords() {
                            return newRecords;
                        }

                        @Override
                        public IndexInfo getIndexInfo() {
                            return newIndexInfo;
                        }

                    });
                }
                logger.debug("index info = {}", newIndexInfo);
                if (isRemoteIndexAquired(clusterConfigInfo, peerDeviceId)) {
                    logger.debug("index aquired");
                    eventBus.post(new FullIndexAquiredEvent() {
                        @Override
                        public String getFolder() {
                            return folder;
                        }
                    });
                }
//                IndexHandler.this.notifyAll();
                markActive();
                synchronized (indexWaitLock) {
                    indexWaitLock.notifyAll();
                }
            }
        }

        public void stop() {
            logger.info("stopping index record processor");
            executorService.shutdown();
            ExecutorUtils.awaitTerminationSafe(executorService);
        }

    }
}
