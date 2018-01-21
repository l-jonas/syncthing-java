/*
 * Copyright 2016 Davide Imbriaco <davide.imbriaco@gmail.com>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syncthing.java.core.utils

import com.google.common.base.Function
import com.google.common.base.Functions
import com.google.common.collect.ComparisonChain
import net.syncthing.java.core.beans.FileInfo

import java.util.Comparator

object FileInfoOrdering {

    private fun isParent(fileInfo: FileInfo) = PathUtils.isParent(fileInfo.path)

    val ALPHA_ASC_DIR_FIRST = Comparator<FileInfo> { a, b ->
        ComparisonChain.start()
                .compareTrueFirst(isParent(a), isParent(b))
                .compare(if (a.isDirectory) 1 else 2, if (b.isDirectory) 1 else 2)
                .compare(a.path, b.path)
                .result()
    }

    val LAST_MOD_DESC = Comparator<FileInfo> { a, b ->
        ComparisonChain.start()
                .compareTrueFirst(isParent(a), isParent(b))
                .compare(b.lastModified, a.lastModified)
                .compare(a.path, b.path)
                .result()
    }

}
