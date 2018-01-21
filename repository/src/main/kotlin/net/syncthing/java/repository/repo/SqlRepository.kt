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
package net.syncthing.java.repository.repo

import com.google.common.base.Objects.equal
import com.google.common.base.Optional
import com.google.common.base.Preconditions.checkArgument
import com.google.common.base.Preconditions.checkNotNull
import com.google.common.base.Strings.emptyToNull
import com.google.common.base.Strings.nullToEmpty
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import com.google.common.eventbus.EventBus
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import net.syncthing.java.bep.BlockExchangeExtraProtos
import net.syncthing.java.bep.BlockExchangeProtos
import net.syncthing.java.core.beans.*
import net.syncthing.java.core.beans.FileInfo.FileType
import net.syncthing.java.core.beans.FileInfo.Version
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.interfaces.DeviceAddressRepository
import net.syncthing.java.core.interfaces.IndexRepository
import net.syncthing.java.core.interfaces.Sequencer
import net.syncthing.java.core.interfaces.TempRepository
import org.apache.commons.lang3.tuple.Pair
import org.apache.http.util.TextUtils.isBlank
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.File
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import java.util.*
import java.util.concurrent.TimeUnit

class SqlRepository(configuration: ConfigurationService) : Closeable, IndexRepository, DeviceAddressRepository, TempRepository {

    private val logger = LoggerFactory.getLogger(javaClass)
    private var sequencer: Sequencer = IndexRepoSequencer()
    private val dataSource: HikariDataSource
    //    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private val eventBus = EventBus()
    private val indexInfoByDeviceIdAndFolder = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS)
            .build(object : CacheLoader<Pair<String, String>, Optional<IndexInfo>>() {
                @Throws(Exception::class)
                override fun load(key: Pair<String, String>): Optional<IndexInfo> {
                    return Optional.fromNullable(doFindIndexInfoByDeviceAndFolder(key.left, key.right))
                }

            })
    private val folderStatsByFolder = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS)
            .build(object : CacheLoader<String, Optional<FolderStats>>() {
                @Throws(Exception::class)
                override fun load(key: String): Optional<FolderStats> {
                    return Optional.fromNullable(doFindFolderStats(key))
                }

            })

    private val connection: Connection
        @Throws(SQLException::class)
        get() = dataSource.connection

    init {
        logger.info("starting sql database")
        val dbDir = File(configuration.database, "h2_index_database")
        dbDir.mkdirs()
        checkArgument(dbDir.isDirectory && dbDir.canWrite())
        val jdbcUrl = "jdbc:h2:file:" + File(dbDir, "index").absolutePath + nullToEmpty(configuration.repositoryH2Config)
        logger.debug("jdbc url = {}", jdbcUrl)
        val hikariConfig = HikariConfig()
        hikariConfig.driverClassName = "org.h2.Driver"
        hikariConfig.jdbcUrl = jdbcUrl
        hikariConfig.minimumIdle = 4
        val newDataSource: HikariDataSource
        newDataSource = HikariDataSource(hikariConfig)
        dataSource = newDataSource
        checkDb()
        recreateTemporaryTables()
        //        scheduledExecutorService.submit(new Runnable() {
        //            @Override
        //            public void run() {
        //                Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
        //            }
        //        });
        //        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
        //            @Override
        //            public void run() {
        //                if (folderStatsDirty) {
        //                    folderStatsDirty = false;
        //                    updateFolderStats();
        //                }
        //            }
        //        }, 15, 30, TimeUnit.SECONDS);
        logger.debug("database ready")
    }

    override fun getEventBus(): EventBus {
        return eventBus
    }

    private fun checkDb() {
        logger.debug("check db")
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT version_number FROM version").use { statement ->
                    val resultSet = statement.executeQuery()
                    checkArgument(resultSet.first())
                    val version = resultSet.getInt(1)
                    checkArgument(version == VERSION, "database version mismatch, expected %s, found %s", VERSION, version)
                    logger.info("database check ok, version = {}", version)
                }
            }
        } catch (ex: SQLException) {
            logger.warn("invalid database, resetting db", ex)
            initDb()
        }

    }

    private fun initDb() {
        logger.info("init db")
        try {
            connection.use { connection -> connection.prepareStatement("DROP ALL OBJECTS").use { prepareStatement -> prepareStatement.execute() } }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

        try {
            connection.use { connection ->
                connection.prepareStatement("CREATE TABLE index_sequence (index_id BIGINT NOT NULL PRIMARY KEY, current_sequence BIGINT NOT NULL)").use { prepareStatement -> prepareStatement.execute() }
                connection.prepareStatement("CREATE TABLE folder_index_info (folder VARCHAR NOT NULL,"
                        + "device_id VARCHAR NOT NULL,"
                        + "index_id BIGINT NOT NULL,"
                        + "local_sequence BIGINT NOT NULL,"
                        + "max_sequence BIGINT NOT NULL,"
                        + "PRIMARY KEY (folder, device_id))").use { prepareStatement -> prepareStatement.execute() }
                connection.prepareStatement("CREATE TABLE folder_stats (folder VARCHAR NOT NULL PRIMARY KEY,"
                        + "file_count BIGINT NOT NULL,"
                        + "dir_count BIGINT NOT NULL,"
                        + "last_update BIGINT NOT NULL,"
                        + "size BIGINT NOT NULL)").use { prepareStatement -> prepareStatement.execute() }
                connection.prepareStatement("CREATE TABLE file_info (folder VARCHAR NOT NULL,"
                        + "path VARCHAR NOT NULL,"
                        + "file_name VARCHAR NOT NULL,"
                        + "parent VARCHAR NOT NULL,"
                        + "size BIGINT,"
                        + "hash VARCHAR,"
                        + "last_modified BIGINT NOT NULL,"
                        + "file_type VARCHAR NOT NULL,"
                        + "version_id BIGINT NOT NULL,"
                        + "version_value BIGINT NOT NULL,"
                        + "is_deleted BOOLEAN NOT NULL,"
                        + "PRIMARY KEY (folder, path))").use { prepareStatement -> prepareStatement.execute() }
                connection.prepareStatement("CREATE TABLE file_blocks (folder VARCHAR NOT NULL,"
                        + "path VARCHAR NOT NULL,"
                        + "hash VARCHAR NOT NULL,"
                        + "size BIGINT NOT NULL,"
                        + "blocks BINARY NOT NULL,"
                        + "PRIMARY KEY (folder, path))").use { prepareStatement -> prepareStatement.execute() }
                connection.prepareStatement("CREATE TABLE device_address (device_id VARCHAR NOT NULL,"
                        + "instance_id BIGINT,"
                        + "address_url VARCHAR NOT NULL,"
                        + "address_producer VARCHAR NOT NULL,"
                        + "address_type VARCHAR NOT NULL,"
                        + "address_score INT NOT NULL,"
                        + "is_working BOOLEAN NOT NULL,"
                        + "last_modified BIGINT NOT NULL,"
                        + "PRIMARY KEY (device_id, address_url))").use { prepareStatement -> prepareStatement.execute() }
                connection.prepareStatement("CREATE INDEX file_info_folder ON file_info (folder)").use { prepareStatement -> prepareStatement.execute() }
                connection.prepareStatement("CREATE INDEX file_info_folder_path ON file_info (folder, path)").use { prepareStatement -> prepareStatement.execute() }
                connection.prepareStatement("CREATE INDEX file_info_folder_parent ON file_info (folder, parent)").use { prepareStatement -> prepareStatement.execute() }
                connection.prepareStatement("CREATE TABLE version (version_number INT NOT NULL)").use { prepareStatement -> prepareStatement.execute() }
                connection.prepareStatement("INSERT INTO index_sequence VALUES (?,?)").use { prepareStatement ->
                    val newIndexId = Math.abs(Random().nextLong()) + 1
                    val newStartingSequence = Math.abs(Random().nextLong()) + 1
                    prepareStatement.setLong(1, newIndexId)
                    prepareStatement.setLong(2, newStartingSequence)
                    checkArgument(prepareStatement.executeUpdate() == 1)
                }
                connection.prepareStatement("INSERT INTO version (version_number) VALUES (?)").use { prepareStatement ->
                    prepareStatement.setInt(1, VERSION)
                    checkArgument(prepareStatement.executeUpdate() == 1)
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

        logger.info("database initialized")
    }

    private fun recreateTemporaryTables() {
        logger.info("recreateTemporaryTables BEGIN")
        try {
            connection.use { connection -> connection.prepareStatement("CREATE CACHED TEMPORARY TABLE IF NOT EXISTS temporary_data (record_key VARCHAR NOT NULL PRIMARY KEY," + "record_data BINARY NOT NULL)").use { prepareStatement -> prepareStatement.execute() } }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

        logger.info("recreateTemporaryTables END")
    }

    override fun getSequencer(): Sequencer {
        return sequencer
    }

    //INDEX INFO
    @Throws(SQLException::class)
    private fun readFolderIndexInfo(resultSet: ResultSet): IndexInfo {
        return IndexInfo.newBuilder()
                .setFolder(resultSet.getString("folder"))
                .setDeviceId(resultSet.getString("device_id"))
                .setIndexId(resultSet.getLong("index_id"))
                .setLocalSequence(resultSet.getLong("local_sequence"))
                .setMaxSequence(resultSet.getLong("max_sequence"))
                .build()
    }

    override fun updateIndexInfo(indexInfo: IndexInfo) {
        try {
            connection.use { connection ->
                connection.prepareStatement("MERGE INTO folder_index_info"
                        + " (folder,device_id,index_id,local_sequence,max_sequence)"
                        + " VALUES (?,?,?,?,?)").use { prepareStatement ->
                    prepareStatement.setString(1, indexInfo.folder)
                    prepareStatement.setString(2, indexInfo.deviceId)
                    prepareStatement.setLong(3, indexInfo.indexId)
                    prepareStatement.setLong(4, indexInfo.localSequence)
                    prepareStatement.setLong(5, indexInfo.maxSequence)
                    prepareStatement.executeUpdate()
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

        indexInfoByDeviceIdAndFolder.put(Pair.of(indexInfo.deviceId, indexInfo.folder), Optional.of(indexInfo))
    }

    override fun findIndexInfoByDeviceAndFolder(deviceId: String, folder: String): IndexInfo? {
        return indexInfoByDeviceIdAndFolder.getUnchecked(Pair.of(deviceId, folder)).orNull()
    }

    private fun doFindIndexInfoByDeviceAndFolder(deviceId: String, folder: String): IndexInfo? {
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT * FROM folder_index_info WHERE device_id=? AND folder=?").use { prepareStatement ->
                    prepareStatement.setString(1, deviceId)
                    prepareStatement.setString(2, folder)
                    val resultSet = prepareStatement.executeQuery()
                    return if (resultSet.first()) {
                        readFolderIndexInfo(resultSet)
                    } else {
                        null
                    }
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

    }

    // FILE INFO
    override fun findFileInfo(folder: String, path: String): FileInfo? {
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT * FROM file_info WHERE folder=? AND path=?").use { prepareStatement ->
                    prepareStatement.setString(1, folder)
                    prepareStatement.setString(2, path)
                    val resultSet = prepareStatement.executeQuery()
                    return if (resultSet.first()) {
                        readFileInfo(resultSet)
                    } else {
                        null
                    }
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

    }

    override fun findFileInfoLastModified(folder: String, path: String): Date? {
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT last_modified FROM file_info WHERE folder=? AND path=?").use { prepareStatement ->
                    prepareStatement.setString(1, folder)
                    prepareStatement.setString(2, path)
                    val resultSet = prepareStatement.executeQuery()
                    return if (resultSet.first()) {
                        Date(resultSet.getLong("last_modified"))
                    } else {
                        null
                    }
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

    }

    override fun findNotDeletedFileInfo(folder: String, path: String): FileInfo? {
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT * FROM file_info WHERE folder=? AND path=? AND is_deleted=FALSE").use { prepareStatement ->
                    prepareStatement.setString(1, folder)
                    prepareStatement.setString(2, path)
                    val resultSet = prepareStatement.executeQuery()
                    return if (resultSet.first()) {
                        readFileInfo(resultSet)
                    } else {
                        null
                    }
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

    }

    @Throws(SQLException::class)
    private fun readFileInfo(resultSet: ResultSet): FileInfo {
        val folder = resultSet.getString("folder")
        val path = resultSet.getString("path")
        val fileType = FileType.valueOf(resultSet.getString("file_type"))
        val lastModified = Date(resultSet.getLong("last_modified"))
        val versionList = listOf(Version(resultSet.getLong("version_id"), resultSet.getLong("version_value")))
        val isDeleted = resultSet.getBoolean("is_deleted")
        val builder = FileInfo.Builder()
                .setFolder(folder)
                .setPath(path)
                .setLastModified(lastModified)
                .setVersionList(versionList)
                .setDeleted(isDeleted)
        return if (equal(fileType, FileType.DIRECTORY)) {
            builder.setTypeDir().build()
        } else {
            builder.setTypeFile().setSize(resultSet.getLong("size")).setHash(resultSet.getString("hash")).build()
        }
    }

    override fun findFileBlocks(folder: String, path: String): FileBlocks? {
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT * FROM file_blocks WHERE folder=? AND path=?").use { prepareStatement ->
                    prepareStatement.setString(1, folder)
                    prepareStatement.setString(2, path)
                    val resultSet = prepareStatement.executeQuery()
                    return if (resultSet.first()) {
                        readFileBlocks(resultSet)
                    } else {
                        null
                    }
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        } catch (ex: InvalidProtocolBufferException) {
            throw RuntimeException(ex)
        }

    }

    @Throws(SQLException::class, InvalidProtocolBufferException::class)
    private fun readFileBlocks(resultSet: ResultSet): FileBlocks {
        val blocks = BlockExchangeExtraProtos.Blocks.parseFrom(resultSet.getBytes("blocks"))
        val blockList = Lists.transform(blocks.blocksList, { record -> BlockInfo(record!!.getOffset(), record.getSize(), BaseEncoding.base16().encode(record.getHash().toByteArray())) })
        return FileBlocks(resultSet.getString("folder"), resultSet.getString("path"), blockList)
    }

    override fun updateFileInfo(newFileInfo: FileInfo, newFileBlocks: FileBlocks?) {
        var folderStats: FolderStats? = null
        val version = Iterables.getLast(newFileInfo.versionList)
        //TODO open transsaction, rollback
        try {
            connection.use { connection ->
                if (newFileBlocks != null) {
                    FileInfo.checkBlocks(newFileInfo, newFileBlocks)
                    connection.prepareStatement("MERGE INTO file_blocks"
                            + " (folder,path,hash,size,blocks)"
                            + " VALUES (?,?,?,?,?)").use { prepareStatement ->
                        prepareStatement.setString(1, newFileBlocks.folder)
                        prepareStatement.setString(2, newFileBlocks.path)
                        prepareStatement.setString(3, newFileBlocks.hash)
                        prepareStatement.setLong(4, newFileBlocks.size)
                        prepareStatement.setBytes(5, BlockExchangeExtraProtos.Blocks.newBuilder()
                                .addAllBlocks(Iterables.transform(newFileBlocks.blocks) { input ->
                                    BlockExchangeProtos.BlockInfo.newBuilder()
                                            .setOffset(input!!.offset)
                                            .setSize(input.size)
                                            .setHash(ByteString.copyFrom(BaseEncoding.base16().decode(input.hash)))
                                            .build()
                                }).build().toByteArray())
                        prepareStatement.executeUpdate()
                    }
                }
                val oldFileInfo = findFileInfo(newFileInfo.folder, newFileInfo.path)
                connection.prepareStatement("MERGE INTO file_info"
                        + " (folder,path,file_name,parent,size,hash,last_modified,file_type,version_id,version_value,is_deleted)"
                        + " VALUES (?,?,?,?,?,?,?,?,?,?,?)").use { prepareStatement ->
                    prepareStatement.setString(1, newFileInfo.folder)
                    prepareStatement.setString(2, newFileInfo.path)
                    prepareStatement.setString(3, newFileInfo.fileName)
                    prepareStatement.setString(4, newFileInfo.parent)
                    prepareStatement.setLong(7, newFileInfo.lastModified.time)
                    prepareStatement.setString(8, newFileInfo.type.name)
                    prepareStatement.setLong(9, version.id)
                    prepareStatement.setLong(10, version.value)
                    prepareStatement.setBoolean(11, newFileInfo.isDeleted)
                    if (newFileInfo.isDirectory) {
                        prepareStatement.setNull(5, Types.BIGINT)
                        prepareStatement.setNull(6, Types.VARCHAR)
                    } else {
                        prepareStatement.setLong(5, newFileInfo.size!!)
                        prepareStatement.setString(6, newFileInfo.hash)
                    }
                    prepareStatement.executeUpdate()
                }
                //update stats
                var deltaFileCount: Long = 0
                var deltaDirCount: Long = 0
                var deltaSize: Long = 0
                val oldMissing = oldFileInfo == null || oldFileInfo.isDeleted
                val newMissing = newFileInfo.isDeleted
                val oldSizeMissing = oldMissing || !oldFileInfo!!.isFile
                val newSizeMissing = newMissing || !newFileInfo.isFile
                if (!oldSizeMissing) {
                    deltaSize -= oldFileInfo!!.size!!
                }
                if (!newSizeMissing) {
                    deltaSize += newFileInfo.size!!
                }
                if (!oldMissing) {
                    if (oldFileInfo!!.isFile) {
                        deltaFileCount--
                    } else if (oldFileInfo.isDirectory) {
                        deltaDirCount--
                    }
                }
                if (!newMissing) {
                    if (newFileInfo.isFile) {
                        deltaFileCount++
                    } else if (newFileInfo.isDirectory) {
                        deltaDirCount++
                    }
                }
                folderStats = updateFolderStats(connection, newFileInfo.folder, deltaFileCount, deltaDirCount, deltaSize, newFileInfo.lastModified)
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

        folderStatsByFolder.put(folderStats!!.folder, Optional.of(folderStats))
        eventBus.post(object : IndexRepository.FolderStatsUpdatedEvent() {
            override fun getFolderStats(): List<FolderStats> {
                return listOf(folderStats!!)
            }
        })
    }

    override fun findNotDeletedFilesByFolderAndParent(folder: String, parentPath: String): MutableList<FileInfo> {
        val list = Lists.newArrayList<FileInfo>()
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT * FROM file_info WHERE folder=? AND parent=? AND is_deleted=FALSE").use { prepareStatement ->
                    prepareStatement.setString(1, folder)
                    prepareStatement.setString(2, parentPath)
                    val resultSet = prepareStatement.executeQuery()
                    while (resultSet.next()) {
                        list.add(readFileInfo(resultSet))
                    }
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

        return list
    }

    override fun findFileInfoBySearchTerm(query: String): List<FileInfo> {
        checkArgument(!isBlank(query))
        //        checkArgument(maxResult > 0);
        val list = Lists.newArrayList<FileInfo>()
        //        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM file_info WHERE LOWER(file_name) LIKE ? AND is_deleted=FALSE LIMIT ?")) {
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT * FROM file_info WHERE LOWER(file_name) REGEXP ? AND is_deleted=FALSE").use { preparedStatement ->
                    //        try (Connection connection = getConnection(); PreparedStatement prepareStatement = connection.prepareStatement("SELECT * FROM file_info LIMIT 10")) {
                    //            preparedStatement.setString(1, "%" + query.trim().toLowerCase() + "%");
                    preparedStatement.setString(1, query.trim { it <= ' ' }.toLowerCase())
                    //            preparedStatement.setInt(2, maxResult);
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        list.add(readFileInfo(resultSet))
                    }
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

        return list
    }

    override fun countFileInfoBySearchTerm(query: String): Long {
        checkArgument(!isBlank(query))
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT COUNT(*) FROM file_info WHERE LOWER(file_name) REGEXP ? AND is_deleted=FALSE").use { preparedStatement ->
                    //        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM file_info")) {
                    preparedStatement.setString(1, query.trim { it <= ' ' }.toLowerCase())
                    val resultSet = preparedStatement.executeQuery()
                    checkArgument(resultSet.first())
                    return resultSet.getLong(1)
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

    }

    // FILE INFO - END
    override fun clearIndex() {
        initDb()
        sequencer = IndexRepoSequencer()
        indexInfoByDeviceIdAndFolder.invalidateAll()
        folderStatsByFolder.invalidateAll()
    }

    // FOLDER STATS - BEGIN
    @Throws(SQLException::class)
    private fun readFolderStats(resultSet: ResultSet): FolderStats {
        return FolderStats.newBuilder()
                .setFolder(resultSet.getString("folder"))
                .setDirCount(resultSet.getLong("dir_count"))
                .setFileCount(resultSet.getLong("file_count"))
                .setSize(resultSet.getLong("size"))
                .setLastUpdate(Date(resultSet.getLong("last_update")))
                .build()
    }

    override fun findFolderStats(folder: String): FolderStats? {
        return folderStatsByFolder.getUnchecked(folder).orNull()
    }

    private fun doFindFolderStats(folder: String): FolderStats? {
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT * FROM folder_stats WHERE folder=?").use { prepareStatement ->
                    prepareStatement.setString(1, folder)
                    val resultSet = prepareStatement.executeQuery()
                    return if (resultSet.first()) {
                        readFolderStats(resultSet)
                    } else {
                        null
                    }
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

    }

    override fun findAllFolderStats(): List<FolderStats> {
        val list = Lists.newArrayList<FolderStats>()
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT * FROM folder_stats").use { prepareStatement ->
                    val resultSet = prepareStatement.executeQuery()
                    while (resultSet.next()) {
                        val folderStats = readFolderStats(resultSet)
                        list.add(folderStats)
                        folderStatsByFolder.put(folderStats.folder, Optional.of(folderStats))
                    }
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

        return list
    }

    @Throws(SQLException::class)
    private fun updateFolderStats(connection: Connection, folder: String, deltaFileCount: Long, deltaDirCount: Long, deltaSize: Long, lastUpdate: Date): FolderStats {
        val oldFolderStats = findFolderStats(folder)
        val newFolderStats: FolderStats
        if (oldFolderStats == null) {
            newFolderStats = FolderStats.newBuilder()
                    .setDirCount(deltaDirCount)
                    .setFileCount(deltaFileCount)
                    .setFolder(folder)
                    .setLastUpdate(lastUpdate)
                    .setSize(deltaSize)
                    .build()
        } else {
            newFolderStats = oldFolderStats.copyBuilder()
                    .setDirCount(oldFolderStats.dirCount + deltaDirCount)
                    .setFileCount(oldFolderStats.fileCount + deltaFileCount)
                    .setSize(oldFolderStats.size + deltaSize)
                    .setLastUpdate(if (lastUpdate.after(oldFolderStats.lastUpdate)) lastUpdate else oldFolderStats.lastUpdate)
                    .build()
        }
        updateFolderStats(connection, newFolderStats)
        return newFolderStats
    }

    //    private void updateFolderStats() {
    //        logger.info("updateFolderStats BEGIN");
    //        final Map<String, FolderStats.Builder> map = Maps.newHashMap();
    //        final Function<String, FolderStats.Builder> func = new Function<String, FolderStats.Builder>() {
    //            @Override
    //            public FolderStats.Builder apply(String folder) {
    //                FolderStats.Builder res = map.get(folder);
    //                if (res == null) {
    //                    res = FolderStats.newBuilder().setFolder(folder);
    //                    map.put(folder, res);
    //                }
    //                return res;
    //            }
    //        };
    //        final List<FolderStats> list;
    //        try (Connection connection = getConnection()) {
    //            try (PreparedStatement prepareStatement = connection.prepareStatement("SELECT folder, COUNT(*) AS file_count, SUM(size) AS size, MAX(last_modified) AS last_update FROM file_info WHERE file_type=? AND is_deleted=FALSE GROUP BY folder")) {
    //                prepareStatement.setString(1, FileType.FILE.name());
    //                ResultSet resultSet = prepareStatement.executeQuery();
    //                while (resultSet.next()) {
    //                    FolderStats.Builder builder = func.apply(resultSet.getString("folder"));
    //                    builder.setSize(resultSet.getLong("size"));
    //                    builder.setFileCount(resultSet.getLong("file_count"));
    //                    builder.setLastUpdate(new Date(resultSet.getLong("last_update")));
    //                }
    //            }
    //            try (PreparedStatement prepareStatement = connection.prepareStatement("SELECT folder, COUNT(*) AS dir_count FROM file_info WHERE file_type=? AND is_deleted=FALSE GROUP BY folder")) {
    //                prepareStatement.setString(1, FileType.DIRECTORY.name());
    //                ResultSet resultSet = prepareStatement.executeQuery();
    //                while (resultSet.next()) {
    //                    FolderStats.Builder builder = func.apply(resultSet.getString("folder"));
    //                    builder.setDirCount(resultSet.getLong("dir_count"));
    //                }
    //            }
    //            list = Lists.newArrayList(Iterables.transform(map.values(), new Function<FolderStats.Builder, FolderStats>() {
    //                @Override
    //                public FolderStats apply(FolderStats.Builder builder) {
    //                    return builder.build();
    //                }
    //            }));
    //            for (FolderStats folderStats : list) {
    //                updateFolderStats(connection, folderStats);
    //            }
    //        } catch (SQLException ex) {
    //            throw new RuntimeException(ex);
    //        }
    //        logger.info("updateFolderStats END");
    //        eventBus.post(new FolderStatsUpdatedEvent() {
    //            @Override
    //            public List<FolderStats> getFolderStats() {
    //                return Collections.unmodifiableList(list);
    //            }
    //        });
    //    }
    @Throws(SQLException::class)
    private fun updateFolderStats(connection: Connection, folderStats: FolderStats) {
        checkArgument(folderStats.fileCount >= 0)
        checkArgument(folderStats.dirCount >= 0)
        checkArgument(folderStats.size >= 0)
        connection.prepareStatement("MERGE INTO folder_stats"
                + " (folder,file_count,dir_count,size,last_update)"
                + " VALUES (?,?,?,?,?)").use { prepareStatement ->
            prepareStatement.setString(1, folderStats.folder)
            prepareStatement.setLong(2, folderStats.fileCount)
            prepareStatement.setLong(3, folderStats.dirCount)
            prepareStatement.setLong(4, folderStats.size)
            prepareStatement.setLong(5, folderStats.lastUpdate.time)
            prepareStatement.executeUpdate()
        }
    }

    override fun close() {
        logger.info("closing index repository (sql)")
        //        scheduledExecutorService.shutdown();
        if (!dataSource.isClosed) {
            dataSource.close()
        }
        //        ExecutorUtils.awaitTerminationSafe(scheduledExecutorService);
    }

    override fun pushTempData(data: ByteArray): String {
        val key = UUID.randomUUID().toString()
        checkNotNull(data)
        try {
            connection.use { connection ->
                connection.prepareStatement("INSERT INTO temporary_data"
                        + " (record_key,record_data)"
                        + " VALUES (?,?)").use { prepareStatement ->
                    prepareStatement.setString(1, key)
                    prepareStatement.setBytes(2, data)
                    prepareStatement.executeUpdate()
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

        return key
    }

    override fun popTempData(key: String): ByteArray {
        checkNotNull<String>(emptyToNull(key))
        try {
            connection.use { connection ->
                var data: ByteArray? = null
                connection.prepareStatement("SELECT record_data FROM temporary_data WHERE record_key = ?").use { statement ->
                    statement.setString(1, key)
                    val resultSet = statement.executeQuery()
                    checkArgument(resultSet.first())
                    data = resultSet.getBytes(1)
                }
                connection.prepareStatement("DELETE FROM temporary_data WHERE record_key = ?").use { statement ->
                    statement.setString(1, key)
                    val count = statement.executeUpdate()
                    checkArgument(count == 1)
                }
                checkNotNull(data)
                return data!!
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

    }

    //SEQUENCER
    private inner class IndexRepoSequencer : Sequencer {

        private var indexId: Long? = null
        private var currentSequence: Long? = null

        @Synchronized private fun loadFromDb() {
            try {
                connection.use { connection ->
                    connection.prepareStatement("SELECT index_id,current_sequence FROM index_sequence").use { statement ->
                        val resultSet = statement.executeQuery()
                        checkArgument(resultSet.first())
                        indexId = resultSet.getLong("index_id")
                        currentSequence = resultSet.getLong("current_sequence")
                        logger.info("loaded index info from db, index_id = {}, current_sequence = {}", indexId, currentSequence)
                    }
                }
            } catch (ex: SQLException) {
                throw RuntimeException(ex)
            }

        }

        @Synchronized override fun indexId(): Long {
            if (indexId == null) {
                loadFromDb()
            }
            return indexId!!
        }

        @Synchronized override fun nextSequence(): Long {
            val nextSequence = currentSequence() + 1
            try {
                connection.use { connection ->
                    connection.prepareStatement("UPDATE index_sequence SET current_sequence=?").use { statement ->
                        statement.setLong(1, nextSequence)
                        checkArgument(statement.executeUpdate() == 1)
                        logger.debug("update local index sequence to {}", nextSequence)
                    }
                }
            } catch (ex: SQLException) {
                throw RuntimeException(ex)
            }

            currentSequence = nextSequence
            return nextSequence
        }

        @Synchronized override fun currentSequence(): Long {
            if (currentSequence == null) {
                loadFromDb()
            }
            return currentSequence!!
        }
    }

    /* device BEGIN */
    @Throws(SQLException::class)
    private fun readDeviceAddress(resultSet: ResultSet): DeviceAddress {
        val instanceId = resultSet.getLong("instance_id")
        return DeviceAddress.newBuilder()
                .setAddress(resultSet.getString("address_url"))
                .setDeviceId(resultSet.getString("device_id"))
                .setInstanceId(if (instanceId == 0L) null else instanceId)
                .setProducer(DeviceAddress.AddressProducer.valueOf(resultSet.getString("address_producer")))
                .setScore(resultSet.getInt("address_score"))
                .setLastModified(Date(resultSet.getLong("last_modified")))
                .build()
    }

    override fun findAllDeviceAddress(): List<DeviceAddress> {
        val list = Lists.newArrayList<DeviceAddress>()
        try {
            connection.use { connection ->
                connection.prepareStatement("SELECT * FROM device_address ORDER BY last_modified DESC").use { prepareStatement ->
                    val resultSet = prepareStatement.executeQuery()
                    while (resultSet.next()) {
                        list.add(readDeviceAddress(resultSet))
                    }
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

        return list
    }

    override fun updateDeviceAddress(deviceAddress: DeviceAddress) {
        try {
            connection.use { connection ->
                connection.prepareStatement("MERGE INTO device_address"
                        + " (device_id,instance_id,address_url,address_producer,address_type,address_score,is_working,last_modified)"
                        + " VALUES (?,?,?,?,?,?,?,?)").use { prepareStatement ->
                    prepareStatement.setString(1, deviceAddress.deviceId)
                    if (deviceAddress.instanceId != null) {
                        prepareStatement.setLong(2, deviceAddress.instanceId!!)
                    } else {
                        prepareStatement.setNull(2, Types.BIGINT)
                    }
                    prepareStatement.setString(3, deviceAddress.address)
                    prepareStatement.setString(4, deviceAddress.producer.name)
                    prepareStatement.setString(5, deviceAddress.type.name)
                    prepareStatement.setInt(6, deviceAddress.score)
                    prepareStatement.setBoolean(7, deviceAddress.isWorking)
                    prepareStatement.setLong(8, deviceAddress.lastModified.time)
                    prepareStatement.executeUpdate()
                }
            }
        } catch (ex: SQLException) {
            throw RuntimeException(ex)
        }

    }

    companion object {
        private val VERSION = 13
    }

    /* device END */
}
