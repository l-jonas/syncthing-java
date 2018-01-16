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

import com.google.common.collect.Iterables
import com.google.common.eventbus.Subscribe
import net.syncthing.java.bep.BlockExchangeConnectionHandler.Companion.assertProtocol
import com.google.common.hash.Hashing
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import net.syncthing.java.bep.BlockExchangeProtos.*
import net.syncthing.java.bep.BlockExchangeProtos.Vector
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.beans.FileInfo.Version
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.security.KeystoreHandler.deviceIdStringToHashData
import net.syncthing.java.core.utils.BlockUtils
import net.syncthing.java.core.utils.FileUtils.createTempFile
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.tuple.Pair
import org.slf4j.LoggerFactory
import java.io.*
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

class BlockPusher internal constructor(private val configuration: ConfigurationService,
                                       private val connectionHandler: BlockExchangeConnectionHandler,
                                       private val indexHandler: IndexHandler) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun pushDelete(fileInfo: FileInfo, folder: String, path: String): IndexEditObserver {
        assertProtocol(connectionHandler.hasFolder(fileInfo.folder), {"supplied connection handler $connectionHandler will not share folder ${fileInfo.folder}"})
        return IndexEditObserver(sendIndexUpdate(folder, BlockExchangeProtos.FileInfo.newBuilder()
                .setName(path)
                .setType(FileInfoType.valueOf(fileInfo.type.name))
                .setDeleted(true), fileInfo.versionList))
    }

    fun pushDir(folder: String, path: String): IndexEditObserver {
        assertProtocol(connectionHandler.hasFolder(folder), {"supplied connection handler $connectionHandler will not share folder $folder"})
        return IndexEditObserver(sendIndexUpdate(folder, BlockExchangeProtos.FileInfo.newBuilder()
                .setName(path)
                .setType(BlockExchangeProtos.FileInfoType.DIRECTORY), null))
    }

    fun pushFile(inputStream: InputStream, fileInfo: FileInfo?, folder: String, path: String): FileUploadObserver {
        try {
            val tempFile = createTempFile(configuration)
            FileUtils.copyInputStreamToFile(inputStream, tempFile)
            logger.debug("use temp file = {} {}", tempFile, FileUtils.byteCountToDisplaySize(tempFile.length()))
            return pushFile(FileDataSource(tempFile), fileInfo, folder, path) //TODO temp file cleanup on complete
            //TODO use mem source on small file
        } catch (ex: IOException) {
            throw RuntimeException(ex)
        }

    }

    fun pushFile(dataSource: DataSource, fileInfo: FileInfo?, folder: String, path: String): FileUploadObserver {
        assertProtocol(connectionHandler.hasFolder(folder), {"supplied connection handler $connectionHandler will not share folder $folder"})
        assert(fileInfo == null || fileInfo.folder == folder)
        assert(fileInfo == null || fileInfo.path == path)
        val monitoringProcessExecutorService = Executors.newCachedThreadPool()
        val fileSize = dataSource.getSize()
        val sentBlocks: MutableSet<String> = Collections.newSetFromMap(ConcurrentHashMap<String, Boolean>())
        val uploadError = AtomicReference<Exception>()
        val isCompleted = AtomicBoolean(false)
        val updateLock = Object()
        val listener = object : Any() {
            @Subscribe
            fun handleRequestMessageReceivedEvent(event: BlockExchangeConnectionHandler.RequestMessageReceivedEvent) {
                val request = event.message
                if (request.folder == folder && request.name == path) {
                    val hash = BaseEncoding.base16().encode(request.hash.toByteArray())
                    logger.debug("handling block request = {}:{}-{} ({})", request.name, request.offset, request.size, hash)
                    val data = dataSource.getBlock(request.offset, request.size, hash)
                    val future = connectionHandler.sendMessage(Response.newBuilder()
                            .setCode(BlockExchangeProtos.ErrorCode.NO_ERROR)
                            .setData(ByteString.copyFrom(data))
                            .setId(request.id)
                            .build())
                    monitoringProcessExecutorService.submit {
                        try {
                            future.get()
                            sentBlocks.add(hash)
                            synchronized(updateLock) {
                                updateLock.notifyAll()
                            }
                            //TODO retry on error, register error and throw on watcher
                        } catch (ex: InterruptedException) {
                            //return and do nothing
                        } catch (ex: ExecutionException) {
                            uploadError.set(ex)
                            synchronized(updateLock) {
                                updateLock.notifyAll()
                            }
                        }
                    }
                }
            }
        }
        connectionHandler.eventBus.register(listener)
        logger.debug("send index update for file = {}", path)
        val indexListener = object : Any() {

            @Subscribe
            fun handleIndexRecordAquiredEvent(event: IndexHandler.IndexRecordAquiredEvent) {
                if (event.folder() == folder) {
                    for (fileInfo2 in event.newRecords()) {
                        if (fileInfo2.path == path && fileInfo2.hash == dataSource.getHash()) { //TODO check not invalid
                            //                                sentBlocks.addAll(dataSource.getHashes());
                            isCompleted.set(true)
                            synchronized(updateLock) {
                                updateLock.notifyAll()
                            }
                        }
                    }
                }
            }
        }
        indexHandler.eventBus.register(indexListener)
        val indexUpdate = sendIndexUpdate(folder, BlockExchangeProtos.FileInfo.newBuilder()
                .setName(path)
                .setSize(fileSize)
                .setType(BlockExchangeProtos.FileInfoType.FILE)
                .addAllBlocks(dataSource.getBlocks()), fileInfo?.versionList).right
        return object : FileUploadObserver() {

            override fun progress() = if (isCompleted.get()) 1.0 else sentBlocks.size / dataSource.getHashes().size.toDouble()

            override fun progressMessage() = (Math.round(progress() * 1000.0) / 10.0).toString() + "% " + sentBlocks.size + "/" + dataSource.getHashes().size

            // return sentBlocks.size() == dataSource.getHashes().size();
            override fun isCompleted() = isCompleted.get()

            override fun dataSource() = dataSource

            override fun close() {
                logger.debug("closing upload process")
                connectionHandler.eventBus.unregister(listener)
                monitoringProcessExecutorService.shutdown()
                indexHandler.eventBus.unregister(indexListener)
                val fileInfo1 = indexHandler.pushRecord(indexUpdate.folder, Iterables.getOnlyElement(indexUpdate.filesList))
                logger.info("sent file info record = {}", fileInfo1)
            }

            @Throws(InterruptedException::class)
            override fun waitForProgressUpdate(): Double {
                synchronized(updateLock) {
                    updateLock.wait()
                }
                if (uploadError.get() != null) {
                    throw RuntimeException(uploadError.get())
                }
                return progress()
            }

        }
    }

    private fun sendIndexUpdate(folder: String, fileInfoBuilder: BlockExchangeProtos.FileInfo.Builder, oldVersions: Iterable<Version>?): Pair<Future<*>, IndexUpdate> {
        run {
            val nextSequence = indexHandler.sequencer().nextSequence()
            val list = oldVersions ?: emptyList()
            logger.debug("version list = {}", list)
            val id = ByteBuffer.wrap(deviceIdStringToHashData(configuration.deviceId)).long
            val version = Counter.newBuilder()
                    .setId(id)
                    .setValue(nextSequence)
                    .build()
            logger.debug("append new version = {}", version)
            fileInfoBuilder
                    .setSequence(nextSequence)
                    .setVersion(Vector.newBuilder().addAllCounters(list.map { record -> Counter.newBuilder().setId(record.id).setValue(record.value).build() }).addCounters(version))
        }
        val lastModified = Date()
        val fileInfo = fileInfoBuilder
                .setModifiedS(lastModified.time / 1000)
                .setModifiedNs((lastModified.time % 1000 * 1000000).toInt())
                .setNoPermissions(true)
                .build()
        val indexUpdate = IndexUpdate.newBuilder()
                .setFolder(folder)
                .addFiles(fileInfo)
                .build()
        logger.debug("index update = {}", fileInfo)
        return Pair.of(connectionHandler.sendMessage(indexUpdate), indexUpdate)
    }

    abstract inner class FileUploadObserver : Closeable {

        abstract fun progress(): Double

        abstract fun progressMessage(): String

        abstract fun isCompleted(): Boolean

        abstract fun dataSource(): DataSource

        @Throws(InterruptedException::class)
        abstract fun waitForProgressUpdate(): Double

        @Throws(InterruptedException::class)
        fun waitForComplete(): FileUploadObserver {
            while (!isCompleted()) {
                waitForProgressUpdate()
            }
            return this
        }
    }

    inner class IndexEditObserver(private val future: Future<*>, val indexUpdate: IndexUpdate) : Closeable {

        //throw exception if job has errors
        val isCompleted: Boolean
            get() {
                return if (future.isDone) {
                    try {
                        future.get()
                    } catch (ex: InterruptedException) {
                        throw RuntimeException(ex)
                    } catch (ex: ExecutionException) {
                        throw RuntimeException(ex)
                    }
                    true
                } else {
                    false
                }
            }

        init {
            checkNotNull(future)
            checkNotNull(indexUpdate)
        }

        constructor(pair: Pair<Future<*>, IndexUpdate>) : this(pair.left, pair.right) {}

        @Throws(InterruptedException::class)
        fun waitForComplete() {
            try {
                future.get()
            } catch (ex: ExecutionException) {
                throw RuntimeException(ex)
            }

        }

        @Throws(IOException::class)
        override fun close() {
            indexHandler.pushRecord(indexUpdate.folder, Iterables.getOnlyElement(indexUpdate.filesList))
        }

    }

    class FileDataSource(private val file: File) : DataSource() {

        override val inputStream: InputStream
            get() {
                try {
                    return FileInputStream(file)
                } catch (ex: FileNotFoundException) {
                    throw RuntimeException(ex)
                }
            }

        override fun getSize(): Long {
            if (size == null) {
                size = file.length()
            }
            return size!!
        }

    }

    abstract class DataSource {

        var size: Long? = null
        private var blocks: List<BlockInfo>? = null
        private var hashes: Set<String>? = null

        abstract val inputStream: InputStream

        @Transient private var hash: String? = null

        private fun processStream() {
            try {
                inputStream.use { it ->
                    val list = mutableListOf<BlockInfo>()
                    var offset: Long = 0
                    while (true) {
                        var block = ByteArray(BLOCK_SIZE)
                        val blockSize = it.read(block)
                        if (blockSize <= 0) {
                            break
                        }
                        if (blockSize < block.size) {
                            block = Arrays.copyOf(block, blockSize)
                        }
                        val hash = Hashing.sha256().hashBytes(block).asBytes()
                        list.add(BlockInfo.newBuilder()
                                .setHash(ByteString.copyFrom(hash))
                                .setOffset(offset)
                                .setSize(blockSize)
                                .build())
                        offset += blockSize.toLong()
                    }
                    size = offset
                    blocks = list
                }
            } catch (ex: IOException) {
                throw RuntimeException(ex)
            }

        }

        open fun getSize(): Long {
            if (size == null) {
                processStream()
            }
            return size!!
        }

        fun getBlocks(): List<BlockInfo>? {
            if (blocks == null) {
                processStream()
            }
            return blocks
        }

        fun getBlock(offset: Long, size: Int, hash: String): ByteArray {
            val buffer = ByteArray(size)
            try {
                inputStream.use { it ->
                    IOUtils.skipFully(it, offset)
                    IOUtils.readFully(it, buffer)
                    assertProtocol(BaseEncoding.base16().encode(Hashing.sha256().hashBytes(buffer).asBytes()) == hash, {"block hash mismatch!"})
                    return buffer
                }
            } catch (ex: IOException) {
                throw RuntimeException(ex)
            }

        }


        fun getHashes(): Set<String> {
            return hashes ?: let {
                val hashes2 = getBlocks()!!.map { input -> BaseEncoding.base16().encode(input.hash.toByteArray()) }.toSet()
                hashes = hashes2
                return hashes2
            }
        }

        fun getHash(): String {
            return hash ?: let {
                val blockInfo = getBlocks()!!.map { input ->
                    net.syncthing.java.core.beans.BlockInfo(input.offset, input.size, BaseEncoding.base16().encode(input.hash.toByteArray()))
                }
                val hash2 = BlockUtils.hashBlocks(blockInfo)
                hash = hash2
                hash2
            }
        }
    }

    companion object {

        val BLOCK_SIZE = 128 * 1024
    }

}
