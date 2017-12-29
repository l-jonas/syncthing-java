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
package net.syncthing.java.core.interfaces;

import com.google.common.eventbus.EventBus;
import net.syncthing.java.core.beans.FileBlocks;
import net.syncthing.java.core.beans.FileInfo;
import net.syncthing.java.core.beans.FolderStats;
import net.syncthing.java.core.beans.IndexInfo;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.List;

/**
 *
 * @author aleph
 */
public interface IndexRepository {

    EventBus getEventBus();

    Sequencer getSequencer();

    void updateIndexInfo(IndexInfo indexInfo);

    @Nullable
    IndexInfo findIndexInfoByDeviceAndFolder(String deviceId, String folder);

    @Nullable
    FileInfo findFileInfo(String folder, String path);

    @Nullable
    Date findFileInfoLastModified(String folder, String path);

    @Nullable
    FileInfo findNotDeletedFileInfo(String folder, String path);

    @Nullable
    FileBlocks findFileBlocks(String folder, String path);

    void updateFileInfo(FileInfo fileInfo, @Nullable FileBlocks fileBlocks);

    List<FileInfo> findNotDeletedFilesByFolderAndParent(String folder, String parentPath);

    void clearIndex();

    @Nullable
    FolderStats findFolderStats(String folder);

    List<FolderStats> findAllFolderStats();

    List<FileInfo> findFileInfoBySearchTerm(String query);

    long countFileInfoBySearchTerm(String query);

    abstract class FolderStatsUpdatedEvent {

        public abstract List<FolderStats> getFolderStats();

    }

}
