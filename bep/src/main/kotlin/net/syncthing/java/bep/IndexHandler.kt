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

import com.google.common.base.Preconditions.checkArgument
import com.google.common.base.Preconditions.checkNotNull
import com.google.common.base.Stopwatch
import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import com.google.common.io.BaseEncoding
import net.syncthing.java.core.beans.*
import net.syncthing.java.core.beans.FileInfo.Version
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.Sequencer
import net.syncthing.java.core.interfaces.TempRepository
import net.syncthing.java.core.security.KeystoreHandler
import net.syncthing.java.core.utils.ExecutorUtils
import org.apache.commons.lang3.tuple.Pair
import org.apache.http.util.TextUtils
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class IndexHandler(private val configuration: ConfigurationService, val indexRepository: IndexRepository,
                   private val tempRepository: TempRepository) : Closeable {
    private val logger = LoggerFactory.getLogger(javaClass)
    val eventBus = EventBus()
    private val folderInfoByFolder = mutableMapOf<String, FolderInfo>()
    private val indexMessageProcessor = IndexMessageProcessor()
    private var lastIndexActivity: Long = 0
    private val writeAccessLock = Object()
    private val indexWaitLock = Object()

    private fun lastActive(): Long = System.currentTimeMillis() - lastIndexActivity

    fun sequencer(): Sequencer = indexRepository.getSequencer()

    fun folderList(): List<String> = folderInfoByFolder.keys.toList()

    fun folderInfoList(): List<FolderInfo> = folderInfoByFolder.values.toList()

    private fun markActive() {
        lastIndexActivity = System.currentTimeMillis()
    }

    init {
        loadFolderInfoFromConfig()
    }

    private fun loadFolderInfoFromConfig() {
        synchronized(writeAccessLock) {
            for (folderInfo in configuration.getFolders()) {
                folderInfoByFolder.put(folderInfo.folder, folderInfo) //TODO reference 'folder info' repository
            }
        }
    }

    @Synchronized
    fun clearIndex() {
        synchronized(writeAccessLock) {
            indexRepository.clearIndex()
            folderInfoByFolder.clear()
            loadFolderInfoFromConfig()
        }
    }

    internal fun isRemoteIndexAquired(clusterConfigInfo: BlockExchangeConnectionHandler.ClusterConfigInfo, peerDeviceId: String): Boolean {
        var ready = true
        for (folder in clusterConfigInfo.sharedFolders) {
            val indexSequenceInfo = indexRepository.findIndexInfoByDeviceAndFolder(peerDeviceId, folder)
            if (indexSequenceInfo == null || indexSequenceInfo.localSequence < indexSequenceInfo.maxSequence) {
                logger.debug("waiting for index on folder = {} sequenceInfo = {}", folder, indexSequenceInfo)
                ready = false
            }
        }
        return ready
    }

    @Throws(InterruptedException::class)
    @JvmOverloads
    fun waitForRemoteIndexAquired(connectionHandler: BlockExchangeConnectionHandler, timeoutSecs: Long? = null): IndexHandler {
        val timeoutMillis = (timeoutSecs ?: DEFAULT_INDEX_TIMEOUT) * 1000
        synchronized(indexWaitLock) {
            while (!isRemoteIndexAquired(connectionHandler.clusterConfigInfo!!, connectionHandler.deviceId())) {
                indexWaitLock.wait(timeoutMillis)
                checkArgument(connectionHandler.getLastActive() < timeoutMillis || lastActive() < timeoutMillis, "unable to aquire index from connection %s, timeout reached!", connectionHandler)
            }
        }
        logger.debug("aquired all indexes on connection {}", connectionHandler)
        return this
    }

    @Subscribe
    fun handleClusterConfigMessageProcessedEvent(event: BlockExchangeConnectionHandler.ClusterConfigMessageProcessedEvent) {
        synchronized(writeAccessLock) {
            for (folderRecord in event.message.foldersList) {
                val folder = folderRecord.id
                val folderInfo = updateFolderInfo(folder, folderRecord.label)
                logger.debug("aquired folder info from cluster config = {}", folderInfo)
                for (deviceRecord in folderRecord.devicesList) {
                    val deviceId = KeystoreHandler.hashDataToDeviceIdString(deviceRecord.id.toByteArray())
                    if (deviceRecord.hasIndexId() && deviceRecord.hasMaxSequence()) {
                        val folderIndexInfo = updateIndexInfo(folder, deviceId, deviceRecord.indexId, deviceRecord.maxSequence, null)
                        logger.debug("aquired folder index info from cluster config = {}", folderIndexInfo)
                    }
                }
            }
        }
    }

    @Subscribe
    fun handleIndexMessageReceivedEvent(event: BlockExchangeConnectionHandler.AnyIndexMessageReceivedEvent<*>) {
        indexMessageProcessor.handleIndexMessageReceivedEvent(event)
    }

    fun pushRecord(folder: String, bepFileInfo: BlockExchangeProtos.FileInfo): FileInfo? {
        var fileBlocks: FileBlocks? = null
        val builder = FileInfo.Builder()
                .setFolder(folder)
                .setPath(bepFileInfo.name)
                .setLastModified(Date(bepFileInfo.modifiedS * 1000 + bepFileInfo.modifiedNs / 1000000))
                .setVersionList((if (bepFileInfo.hasVersion()) bepFileInfo.version.countersList else null ?: emptyList()).map { record -> Version(record.id, record.value) })
                .setDeleted(bepFileInfo.deleted)
        when (bepFileInfo.type) {
            BlockExchangeProtos.FileInfoType.FILE -> {
                fileBlocks = FileBlocks(folder, builder.getPath()!!, ((bepFileInfo.blocksList ?: emptyList())).map { record ->
                    BlockInfo(record.offset, record.size, BaseEncoding.base16().encode(record.hash.toByteArray()))
                })
                builder
                        .setTypeFile()
                        .setHash(fileBlocks.hash)
                        .setSize(bepFileInfo.size)
            }
            BlockExchangeProtos.FileInfoType.DIRECTORY -> builder.setTypeDir()
            else -> {
                logger.warn("unsupported file type = {}, discarding file info", bepFileInfo.type)
                return null
            }
        }
        return addRecord(builder.build(), fileBlocks)
    }

    private fun updateIndexInfo(folder: String, deviceId: String, indexId: Long?, maxSequence: Long?, localSequence: Long?): IndexInfo {
        synchronized(writeAccessLock) {
            var indexSequenceInfo = indexRepository.findIndexInfoByDeviceAndFolder(deviceId, folder)
            var shouldUpdate = false
            val builder: IndexInfo.Builder
            if (indexSequenceInfo == null) {
                shouldUpdate = true
                assert(indexId != null, {"index sequence info not found, and supplied null index id (folder = $folder, device = $deviceId)"})
                builder = IndexInfo.newBuilder()
                        .setFolder(folder)
                        .setDeviceId(deviceId)
                        .setIndexId(indexId!!)
                        .setLocalSequence(0)
                        .setMaxSequence(-1)
            } else {
                builder = indexSequenceInfo.copyBuilder()
            }
            if (indexId != null && indexId != builder.getIndexId()) {
                shouldUpdate = true
                builder.setIndexId(indexId)
            }
            if (maxSequence != null && maxSequence > builder.getMaxSequence()) {
                shouldUpdate = true
                builder.setMaxSequence(maxSequence)
            }
            if (localSequence != null && localSequence > builder.getLocalSequence()) {
                shouldUpdate = true
                builder.setLocalSequence(localSequence)
            }
            if (shouldUpdate) {
                indexSequenceInfo = builder.build()
                indexRepository.updateIndexInfo(indexSequenceInfo)
            }
            return indexSequenceInfo!!
        }
    }

    private fun addRecord(record: FileInfo, fileBlocks: FileBlocks?): FileInfo? {
        synchronized(writeAccessLock) {
            val lastModified = indexRepository.findFileInfoLastModified(record.folder, record.path)
            if (lastModified != null && !record.lastModified.after(lastModified)) {
                logger.trace("discarding record = {}, modified before local record", record)
                return null
            } else {
                indexRepository.updateFileInfo(record, fileBlocks)
                logger.trace("loaded new record = {}", record)
                eventBus.post(object : IndexChangedEvent() {
                    override val folder: String
                        get() = record.folder

                    override val newRecords: List<FileInfo>
                        get() = listOf(record)
                })
                return record
            }
        }
    }

    fun getFileInfoByPath(folder: String, path: String): FileInfo? {
        return indexRepository.findFileInfo(folder, path)
    }

    fun getFileInfoAndBlocksByPath(folder: String, path: String): Pair<FileInfo, FileBlocks>? {
        val fileInfo = getFileInfoByPath(folder, path)
        return if (fileInfo == null) {
            null
        } else {
            checkArgument(fileInfo.isFile)
            val fileBlocks = indexRepository.findFileBlocks(folder, path)
            checkNotNull(fileBlocks, "file blocks not found for file info = %s", fileInfo)
            Pair.of(fileInfo, fileBlocks)
        }
    }

    private fun updateFolderInfo(folder: String, label: String?): FolderInfo {
        var folderInfo: FolderInfo? = folderInfoByFolder[folder]
        if (folderInfo == null || !TextUtils.isEmpty(label)) {
            folderInfo = FolderInfo(folder, label)
            folderInfoByFolder.put(folderInfo.folder, folderInfo)
        }
        return folderInfo
    }

    fun getFolderInfo(folder: String): FolderInfo? {
        return folderInfoByFolder[folder]
    }

    fun getIndexInfo(device: String, folder: String): IndexInfo? {
        return indexRepository.findIndexInfoByDeviceAndFolder(device, folder)
    }

    fun newFolderBrowser(): FolderBrowser {
        return FolderBrowser(this)
    }

    fun newIndexBrowser(folder: String, includeParentInList: Boolean = false, allowParentInRoot: Boolean = false,
                        ordering: Comparator<FileInfo>? = null): IndexBrowser {
        return IndexBrowser(indexRepository, this, folder, includeParentInList, allowParentInRoot, ordering)
    }

    override fun close() {
        indexMessageProcessor.stop()
    }

    abstract inner class IndexRecordAquiredEvent {

        private var affectedPaths: MutableSet<String>? = null

        abstract fun folder(): String

        abstract fun newRecords(): List<FileInfo>

        abstract fun indexInfo(): IndexInfo

        @Synchronized
        fun getAffectedPaths(): Set<String> {
            if (affectedPaths == null) {
                affectedPaths = mutableSetOf()
                for (fileInfo in newRecords()) {
                    affectedPaths!!.add(fileInfo.path)
                    affectedPaths!!.add(fileInfo.parent)
                }
            }
            return affectedPaths!!
        }

    }

    abstract inner class IndexChangedEvent {

        abstract val folder: String

        abstract val newRecords: List<FileInfo>

    }

    abstract inner class FullIndexAquiredEvent {

        abstract val folder: String

    }

    private inner class IndexMessageProcessor {

        private val logger = LoggerFactory.getLogger(javaClass)
        private val executorService = Executors.newSingleThreadExecutor()
        private var queuedMessages = 0
        private var queuedRecords: Long = 0
        //        private long lastRecordProcessingTime = 0;
        //        , delay = 0;
        //        private boolean addProcessingDelayForInterface = true;
        //        private final int MIN_DELAY = 0, MAX_DELAY = 5000, MAX_RECORD_PER_PROCESS = 16, DELAY_FACTOR = 1;
        private var stopwatch: Stopwatch? = null

        fun handleIndexMessageReceivedEvent(event: BlockExchangeConnectionHandler.AnyIndexMessageReceivedEvent<*>) {
            logger.info("received index message event, preparing (queued records = {} event record count = {})", queuedRecords, event.filesList.size)
            markActive()
            val clusterConfigInfo = event.connectionHandler.clusterConfigInfo
            val peerDeviceId = event.connectionHandler.deviceId()
            //            List<BlockExchangeProtos.FileInfo> fileList = event.getFilesList();
            //            for (int index = 0; index < fileList.size(); index += MAX_RECORD_PER_PROCESS) {
            //                BlockExchangeProtos.IndexUpdate data = BlockExchangeProtos.IndexUpdate.newBuilder()
            //                    .addAllFiles(Iterables.limit(Iterables.skip(fileList, index), MAX_RECORD_PER_PROCESS))
            //                    .setFolder(event.getFolder())
            //                    .build();
            //                if (queuedMessages > 0) {
            //                    storeAndProcessBg(data, clusterConfigInfo, peerDeviceId);
            //                } else {
            //                    processBg(data, clusterConfigInfo, peerDeviceId);
            //                }
            //            }
            val data = BlockExchangeProtos.IndexUpdate.newBuilder()
                    .addAllFiles(event.filesList)
                    .setFolder(event.folder)
                    .build()
            if (queuedMessages > 0) {
                storeAndProcessBg(data, clusterConfigInfo, peerDeviceId)
            } else {
                processBg(data, clusterConfigInfo, peerDeviceId)
            }
        }

        private fun processBg(data: BlockExchangeProtos.IndexUpdate, clusterConfigInfo: BlockExchangeConnectionHandler.ClusterConfigInfo?, peerDeviceId: String) {
            logger.debug("received index message event, queuing for processing")
            queuedMessages++
            queuedRecords += data.filesCount.toLong()
            executorService.submit(object : ProcessingRunnable() {
                override fun runProcess() {
                    doHandleIndexMessageReceivedEvent(data, clusterConfigInfo, peerDeviceId)
                }

            })
        }

        private fun storeAndProcessBg(data: BlockExchangeProtos.IndexUpdate, clusterConfigInfo: BlockExchangeConnectionHandler.ClusterConfigInfo?, peerDeviceId: String) {
            val key = tempRepository.pushTempData(data.toByteArray())
            logger.debug("received index message event, stored to temp record {}, queuing for processing", key)
            queuedMessages++
            queuedRecords += data.filesCount.toLong()
            executorService.submit(object : ProcessingRunnable() {
                override fun runProcess() {
                    try {
                        doHandleIndexMessageReceivedEvent(key, clusterConfigInfo, peerDeviceId)
                    } catch (ex: IOException) {
                        logger.error("error processing index message", ex)
                    }

                }

            })
        }

        private abstract inner class ProcessingRunnable : Runnable {

            override fun run() {
                stopwatch = Stopwatch.createStarted()
                runProcess()
                queuedMessages--
                //                lastRecordProcessingTime = stopwatch.elapsed(TimeUnit.MILLISECONDS) - delay;
                //                logger.info("processed a bunch of records, {}*{} remaining", queuedMessages, MAX_RECORD_PER_PROCESS);
                //                logger.debug("processed index message in {} secs", lastRecordProcessingTime / 1000d);
                stopwatch = null
            }

            protected abstract fun runProcess()

            //        private boolean isVersionOlderThanSequence(BlockExchangeProtos.FileInfo fileInfo, long localSequence) {
            //            long fileSequence = fileInfo.getSequence();
            //            //TODO should we check last version instead of sequence? verify
            //            return fileSequence < localSequence;
            //        }
            @Throws(IOException::class)
            protected fun doHandleIndexMessageReceivedEvent(key: String, clusterConfigInfo: BlockExchangeConnectionHandler.ClusterConfigInfo?, peerDeviceId: String) {
                logger.debug("processing index message event from temp record {}", key)
                markActive()
                val data = tempRepository.popTempData(key)
                val message = BlockExchangeProtos.IndexUpdate.parseFrom(data)
                doHandleIndexMessageReceivedEvent(message, clusterConfigInfo, peerDeviceId)
            }

            protected fun doHandleIndexMessageReceivedEvent(message: BlockExchangeProtos.IndexUpdate, clusterConfigInfo: BlockExchangeConnectionHandler.ClusterConfigInfo?, peerDeviceId: String) {
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
                logger.info("processing index message with {} records (queue size: messages = {} records = {})", message.filesCount, queuedMessages, queuedRecords)
                //            String deviceId = connectionHandler.getDeviceId();
                val folder = message.folder
                var sequence: Long = -1
                val newRecords = mutableListOf<FileInfo>()
                //                IndexInfo oldIndexInfo = indexRepository.findIndexInfoByDeviceAndFolder(deviceId, folder);
                //            Stopwatch stopwatch = Stopwatch.createStarted();
                logger.debug("processing {} index records for folder {}", message.filesList.size, folder)
                for (fileInfo in message.filesList) {
                    markActive()
                    //                    if (oldIndexInfo != null && isVersionOlderThanSequence(fileInfo, oldIndexInfo.getLocalSequence())) {
                    //                        logger.trace("skipping file {}, version older than sequence {}", fileInfo, oldIndexInfo.getLocalSequence());
                    //                    } else {
                    val newRecord = pushRecord(folder, fileInfo)
                    if (newRecord != null) {
                        newRecords.add(newRecord)
                    }
                    sequence = Math.max(fileInfo.sequence, sequence)
                    markActive()
                    //                    }
                }
                val newIndexInfo = updateIndexInfo(folder, peerDeviceId, null, null, sequence)
                val elap = stopwatch!!.elapsed(TimeUnit.MILLISECONDS)
                queuedRecords -= message.filesCount.toLong()
                logger.info("processed {} index records, aquired {} ({} secs, {} record/sec)", message.filesCount, newRecords.size, elap / 1000.0, Math.round(message.filesCount / (elap / 1000.0) * 100) / 100.0)
                logger.info("remaining queue size: messages = {} records = {}; eta {} min", queuedMessages, queuedRecords, Math.round(queuedRecords / message.filesCount * (elap / 1000.0)) / 60.0)
                if (logger.isInfoEnabled && newRecords.size <= 10) {
                    for (fileInfo in newRecords) {
                        logger.info("aquired record = {}", fileInfo)
                    }
                }
                if (!newRecords.isEmpty()) {
                    eventBus.post(object : IndexRecordAquiredEvent() {
                        override fun folder() = folder

                        override fun newRecords() = newRecords

                        override fun indexInfo() = newIndexInfo

                    })
                }
                logger.debug("index info = {}", newIndexInfo)
                if (isRemoteIndexAquired(clusterConfigInfo!!, peerDeviceId)) {
                    logger.debug("index aquired")
                    eventBus.post(object : FullIndexAquiredEvent() {
                        override val folder: String
                            get() = folder
                    })
                }
                //                IndexHandler.this.notifyAll();
                markActive()
                synchronized(indexWaitLock) {
                    indexWaitLock.notifyAll()
                }
            }
        }

        fun stop() {
            logger.info("stopping index record processor")
            executorService.shutdown()
            ExecutorUtils.awaitTerminationSafe(executorService)
        }

    }

    companion object {

        private val DEFAULT_INDEX_TIMEOUT: Long = 30
    }
}
