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

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.eventbus.Subscribe
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.utils.ExecutorUtils
import net.syncthing.java.core.utils.FileInfoOrdering.ALPHA_ASC_DIR_FIRST
import net.syncthing.java.core.utils.PathUtils
import net.syncthing.java.core.utils.PathUtils.*
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class IndexBrowser internal constructor(private val indexRepository: IndexRepository, private val indexHandler: IndexHandler,
                                       val folder: String, private val includeParentInList: Boolean = false,
                                       private val allowParentInRoot: Boolean = false, ordering: Comparator<FileInfo>?) : Closeable {

    private val ordering = ordering ?: ALPHA_ASC_DIR_FIRST
    private val logger = LoggerFactory.getLogger(javaClass)
    private val listFolderCache = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            //        .weigher(new Weigher<String, List<FileInfo>>() {
            //            @Override
            //            public int weigh(String key, List<FileInfo> list) {
            //                return list.size();
            //            }
            //        })
            //        .maximumSize(1000)
            .build(object : CacheLoader<String, List<FileInfo>>() {
                @Throws(Exception::class)
                override fun load(path: String): List<FileInfo> {
                    return doListFiles(path)
                }

            })
    private val fileInfoCache = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            //        .maximumSize(1000)
            //        .weigher(new Weigher<String, FileInfo>() {
            //            @Override
            //            public int weigh(String key, FileInfo fileInfo) {
            //                return fileInfo.getBlocks().size();
            //            }
            //        })
            .build(object : CacheLoader<String, FileInfo>() {
                @Throws(Exception::class)
                override fun load(path: String): FileInfo? {
                    return doGetFileInfoByAbsolutePath(path)
                }
            })
    var currentPath: String = ROOT_PATH
        private set
    private val PARENT_FILE_INFO: FileInfo
    private val ROOT_FILE_INFO: FileInfo
    private val executorService = Executors.newSingleThreadScheduledExecutor()
    private val indexHandlerEventListener = object : Any() {
        @Subscribe
        fun handleIndexChangedEvent(event: IndexHandler.IndexChangedEvent) {
            if (event.folder == folder) {
                invalidateCache()
            }
        }

    }
    private val preloadJobs = mutableSetOf<String>()
    private val preloadJobsLock = Any()
    private var mOnPathChangedListener: (() -> Unit)? = null

    private fun isCacheReady(): Boolean {
        synchronized(preloadJobsLock) {
            return preloadJobs.isEmpty()
        }
    }

    fun currentPathInfo(): FileInfo = getFileInfoByAbsolutePath(currentPath)

    fun currentPathFileName(): String? = PathUtils.getFileName(currentPath)

    fun isRoot(): Boolean = PathUtils.isRoot(currentPath)

    init {
        assert(folder.isNotEmpty())
        this.indexHandler.eventBus.register(indexHandlerEventListener)
        PARENT_FILE_INFO = FileInfo.Builder()
                .setFolder(folder)
                .setTypeDir()
                .setPath(PARENT_PATH)
                .build()
        ROOT_FILE_INFO = FileInfo.Builder()
                .setFolder(folder)
                .setTypeDir()
                .setPath(ROOT_PATH)
                .build()
        executorService.scheduleWithFixedDelay({
            logger.debug("folder cache cleanup")
            listFolderCache.cleanUp()
            fileInfoCache.cleanUp()
        }, 1, 1, TimeUnit.MINUTES)
        navigateToAbsolutePath(ROOT_PATH)
    }

    fun setOnFolderChangedListener(onPathChangedListener: (() -> Unit)?) {
        mOnPathChangedListener = onPathChangedListener
    }

    private fun invalidateCache() {
        listFolderCache.invalidateAll()
        fileInfoCache.invalidateAll()
        preloadFileInfoForCurrentPath()
    }

    private fun preloadFileInfoForCurrentPath() {
        logger.debug("trigger preload for folder = '{}'", folder)
        synchronized(preloadJobsLock) {
            currentPath.let<String, Any> { currentPath ->
                if (preloadJobs.contains(currentPath)) {
                    preloadJobs.remove(currentPath)
                    preloadJobs.add(currentPath) ///add last
                } else {
                    preloadJobs.add(currentPath)
                    executorService.submit(object : Runnable {

                        override fun run() {

                            var preloadPath: String? = null
                            synchronized(preloadJobsLock) {
                                assert(!preloadJobs.isEmpty())
                                preloadPath = preloadJobs.last() //pop last job
                            }

                            logger.info("folder preload BEGIN for folder = '{}' path = '{}'", folder, preloadPath)
                            getFileInfoByAbsolutePath(preloadPath)
                            if (!PathUtils.isRoot(preloadPath)) {
                                val parent = getParentPath(preloadPath)
                                getFileInfoByAbsolutePath(parent)
                                listFiles(parent)
                            }
                            for (record in listFiles(preloadPath)) {
                                if (record.path == PARENT_FILE_INFO.path && record.isDirectory) {
                                    listFiles(record.path)
                                }
                            }
                            logger.info("folder preload END for folder = '{}' path = '{}'", folder, preloadPath)
                            synchronized(preloadJobsLock) {
                                preloadJobs.remove(preloadPath)
                                if (isCacheReady()) {
                                    logger.info("cache ready, notify listeners")
                                    mOnPathChangedListener?.invoke()
                                } else {
                                    logger.info("still {} job[s] left in cache loader", preloadJobs.size)
                                    executorService.submit(this)
                                }
                            }
                        }
                    })
                }
            }
        }
    }

    fun listFiles(absoluteDirPath: String? = currentPath): List<FileInfo> {
        logger.debug("listFiles for path = '{}'", absoluteDirPath)
        return listFolderCache.getUnchecked(absoluteDirPath!!)
    }

    private fun doListFiles(path: String): List<FileInfo> {
        logger.debug("doListFiles for path = '{}' BEGIN", path)
        val list = indexRepository.findNotDeletedFilesByFolderAndParent(folder, path)
        logger.debug("doListFiles for path = '{}' : {} records loaded)", path, list.size)
        for (fileInfo in list) {
            fileInfoCache.put(fileInfo.path, fileInfo)
        }
        Collections.sort(list, ordering)
        if (includeParentInList && (!PathUtils.isRoot(path) || allowParentInRoot)) {
            list.add(0, PARENT_FILE_INFO)
        }
        return Collections.unmodifiableList(list)
    }

    fun getFileInfoByAbsolutePath(path: String?): FileInfo {
        return if (PathUtils.isRoot(path)) ROOT_FILE_INFO else fileInfoCache.getUnchecked(path!!)
    }

    private fun doGetFileInfoByAbsolutePath(path: String): FileInfo? {
        logger.debug("doGetFileInfoByAbsolutePath for path = '{}' BEGIN", path)
        val fileInfo = indexRepository.findNotDeletedFileInfo(folder, path) ?: error("file not found for path = $path")
        logger.debug("doGetFileInfoByAbsolutePath for path = '{}' END", path)
        return fileInfo
    }

    fun navigateTo(fileInfo: FileInfo) {
        assert(fileInfo.isDirectory)
        assert(fileInfo.folder == folder)
        return if (fileInfo.path == PARENT_FILE_INFO.path)
            navigateToAbsolutePath(getParentPath(currentPath))
        else
            navigateToAbsolutePath(fileInfo.path)
    }

    fun navigateToNearestPath(oldPath: String) {
        if (!StringUtils.isBlank(oldPath)) {
            navigateToAbsolutePath(oldPath)
        }
    }

    private fun navigateToAbsolutePath(newPath: String) {
        if (PathUtils.isRoot(newPath)) {
            currentPath = ROOT_PATH
        } else {
            val fileInfo = getFileInfoByAbsolutePath(newPath)
            assert(fileInfo.isDirectory, {"cannot navigate to path ${fileInfo.path}: not a directory"})
            currentPath = fileInfo.path
        }
        logger.info("navigate to path = '{}'", currentPath)
        preloadFileInfoForCurrentPath()
    }

    override fun close() {
        logger.info("closing")
        this.indexHandler.eventBus.unregister(indexHandlerEventListener)
        executorService.shutdown()
        ExecutorUtils.awaitTerminationSafe(executorService)
    }
}
