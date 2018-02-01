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
package net.syncthing.java.core.cache

import net.syncthing.java.core.utils.PathUtils
import net.syncthing.java.core.utils.submitLogging
import org.apache.commons.io.FileUtils
import org.bouncycastle.util.encoders.Hex
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.security.MessageDigest
import java.util.concurrent.Executors

internal class FileBlockCache(private val dir: File) : BlockCache() {

    private val logger = LoggerFactory.getLogger(javaClass)
    private var size: Long = 0

    init {
        size = sizeOfDirectory(dir)
        runCleanup()
    }

    /**
     * Replacement for [[FileUtils.sizeOfDirectory]], which throws exceptions on Android.
     */
    private fun sizeOfDirectory(dir: File): Long {
        if (!dir.exists())
            return 0

        return dir.listFiles()?.toList()
                ?.map { // Recursive call if it's a directory
                    if (it.isDirectory) {
                        sizeOfDirectory(it)
                    } else {
                        // Sum the file size in bytes
                        it.length()
                    }
                }
                ?.sum()
                ?: 0
    }

    override fun pushBlock(data: ByteArray): String? {
        val code = Hex.toHexString(MessageDigest.getInstance("SHA-256").digest(data))
        return if (pushData(code, data)) code else null
    }

    private fun runCleanup() {
        val MAX_SIZE = (50 * 1024 * 1024).toLong()
        if (size > MAX_SIZE) {
            logger.info("starting cleanup of cache directory, initial size = {}", FileUtils.byteCountToDisplaySize(size))
            val files = mutableListOf(*dir.listFiles()!!)
            files.sortBy { it.lastModified() }
            val PERC_TO_DELETE = 0.5
            for (file in files.take((files.size * PERC_TO_DELETE).toInt())) {
                logger.debug("delete file {}", file)
                FileUtils.deleteQuietly(file)
            }
            size = sizeOfDirectory(dir)
            logger.info("cleanup of cache directory completed, final size = {}", FileUtils.byteCountToDisplaySize(size))
        }
    }

    override fun pullBlock(code: String): ByteArray? {
        return pullFile(code, true)
    }

    private fun pullFile(code: String, shouldCheck: Boolean): ByteArray? {
        val file = File(dir, code)
        if (file.exists()) {
            try {
                val data = FileUtils.readFileToByteArray(file)
                if (shouldCheck) {
                    val cachedDataCode = Hex.toHexString(MessageDigest.getInstance("SHA-256").digest(data))
                    assert(code == cachedDataCode, {"cached data code $cachedDataCode does not match code $code"})
                }
                writerThread.submitLogging {
                    try {
                        FileUtils.touch(file)
                    } catch (ex: IOException) {
                        logger.warn("unable to 'touch' file {}", file)
                        logger.warn("unable to 'touch' file", ex)
                    }
                }
                logger.debug("read block {} from cache file {}", code, file)
                return data
            } catch (ex: IOException) {
                logger.warn("error reading block from cache", ex)
                FileUtils.deleteQuietly(file)
            }
        }
        return null
    }

    //        @Override
    //        public void close() {
    //            writerThread.shutdown();
    //            try {
    //                writerThread.awaitTermination(2, TimeUnit.SECONDS);
    //            } catch (InterruptedException ex) {
    //                logger.warn("pending threads on block cache writer executor");
    //            }
    //        }
    override fun pushData(code: String, data: ByteArray): Boolean {
        writerThread.submitLogging {
            val file = File(dir, code)
            if (!file.exists()) {
                try {
                    FileUtils.writeByteArrayToFile(file, data)
                    logger.debug("cached block {} to file {}", code, file)
                    size += data.size.toLong()
                    runCleanup()
                } catch (ex: IOException) {
                    logger.warn("error writing block in cache", ex)
                    FileUtils.deleteQuietly(file)
                }

            }
        }
        return true
    }

    override fun pullData(code: String): ByteArray? {
        return pullFile(code, false)
    }

    override fun clear() {
        writerThread.submitLogging {
            FileUtils.deleteQuietly(dir)
            dir.mkdirs()
        }
    }

    companion object {

        private val writerThread = Executors.newSingleThreadExecutor()
    }
}
