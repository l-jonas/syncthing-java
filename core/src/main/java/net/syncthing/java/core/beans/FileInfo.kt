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
package net.syncthing.java.core.beans

import com.google.common.base.MoreObjects
import com.google.common.base.Strings
import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import net.syncthing.java.core.utils.PathUtils
import org.apache.commons.io.FileUtils
import java.util.Collections
import java.util.Date

import com.google.common.base.Objects.equal
import com.google.common.base.Preconditions.checkArgument
import com.google.common.base.Preconditions.checkNotNull
import com.google.common.base.Strings.emptyToNull

class FileInfo private constructor(val folder: String, val type: FileType, val path: String, size: Long?, lastModified: Date?, hash: String?, versionList: List<Version>?, val isDeleted: Boolean) {
    val fileName: String
    val parent: String
    val hash: String?
    val size: Long?
    val lastModified: Date
    val versionList: List<Version>

    val isDirectory: Boolean
        get() = equal(type, FileType.DIRECTORY)

    val isFile: Boolean
        get() = equal(type, FileType.FILE)

    init {
        checkNotNull<String>(Strings.emptyToNull(folder))
        checkNotNull(path)//allow empty for 'fake' root path
        checkNotNull(type)
        if (PathUtils.isParent(path)) {
            this.fileName = PathUtils.PARENT_PATH
            this.parent = PathUtils.ROOT_PATH
        } else {
            this.fileName = PathUtils.getFileName(path)
            this.parent = if (PathUtils.isRoot(path)) PathUtils.ROOT_PATH else PathUtils.getParentPath(path)
        }
        this.lastModified = MoreObjects.firstNonNull(lastModified, Date(0))
        if (type == FileType.DIRECTORY) {
            this.size = null
            this.hash = null
        } else {
            checkNotNull<Long>(size)
            checkNotNull<String>(emptyToNull(hash))
            this.size = size
            this.hash = hash
        }
        this.versionList = Collections.unmodifiableList(MoreObjects.firstNonNull(versionList, emptyList()))
    }

    enum class FileType {
        FILE, DIRECTORY
    }

    fun describeSize(): String {
        return if (isFile) FileUtils.byteCountToDisplaySize(size!!) else ""
    }

    override fun toString(): String {
        return "FileRecord{" + "folder=" + folder + ", path=" + path + ", size=" + size + ", lastModified=" + lastModified + ", type=" + type + ", last version = " + Iterables.getLast(versionList, null) + '}'
    }

    class Version(val id: Long, val value: Long) {

        override fun toString(): String {
            return "Version{id=$id, value=$value}"
        }

    }

    class Builder {

        private var folder: String? = null
        private var path: String? = null
        private var hash: String? = null
        private var size: Long? = null
        private var lastModified = Date(0)
        private var type: FileType? = null
        var versionList: List<Version>? = null
            private set
        private var deleted = false

        fun getFolder(): String? {
            return folder
        }

        fun setFolder(folder: String): Builder {
            this.folder = folder
            return this
        }

        fun getPath(): String? {
            return path
        }

        fun setPath(path: String): Builder {
            this.path = path
            return this
        }

        fun getSize(): Long? {
            return size
        }

        fun setSize(size: Long?): Builder {
            this.size = size
            return this
        }

        fun getLastModified(): Date {
            return lastModified
        }

        fun setLastModified(lastModified: Date): Builder {
            this.lastModified = lastModified
            return this
        }

        fun getType(): FileType? {
            return type
        }

        fun setType(type: FileType): Builder {
            this.type = type
            return this
        }

        fun setTypeFile(): Builder {
            return setType(FileType.FILE)
        }

        fun setTypeDir(): Builder {
            return setType(FileType.DIRECTORY)
        }

        fun setVersionList(versionList: Iterable<Version>?): Builder {
            this.versionList = if (versionList == null) null else Lists.newArrayList(versionList)
            return this
        }

        fun isDeleted(): Boolean {
            return deleted
        }

        fun setDeleted(deleted: Boolean): Builder {
            this.deleted = deleted
            return this
        }

        fun getHash(): String? {
            return hash
        }

        fun setHash(hash: String): Builder {
            this.hash = hash
            return this
        }

        fun build(): FileInfo {
            return FileInfo(folder!!, type!!, path!!, size, lastModified, hash, versionList, deleted)
        }

    }

    companion object {

        fun checkBlocks(fileInfo: FileInfo, fileBlocks: FileBlocks) {
            checkArgument(equal(fileBlocks.folder, fileInfo.folder), "file info folder not match file block folder")
            checkArgument(equal(fileBlocks.path, fileInfo.path), "file info path does not match file block path")
            checkArgument(fileInfo.isFile, "file info must be of type 'FILE' to have blocks")
            checkArgument(equal(fileBlocks.size, fileInfo.size), "file info size does not match file block size")
            checkArgument(equal(fileBlocks.hash, fileInfo.hash), "file info hash does not match file block hash")
        }
    }

}
