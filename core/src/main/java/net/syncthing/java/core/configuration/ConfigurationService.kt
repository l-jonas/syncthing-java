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
package net.syncthing.java.core.configuration

import com.google.common.base.Joiner
import com.google.common.base.MoreObjects
import com.google.common.base.Strings
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import com.google.common.io.BaseEncoding
import com.google.gson.Gson
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.configuration.gsonbeans.DeviceConfig
import net.syncthing.java.core.configuration.gsonbeans.DeviceConfigList
import net.syncthing.java.core.configuration.gsonbeans.FolderConfig
import net.syncthing.java.core.configuration.gsonbeans.FolderConfigList
import net.syncthing.java.core.utils.ExecutorUtils
import org.apache.commons.io.FileUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.*
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

import com.google.common.base.Objects.equal
import com.google.common.base.Preconditions.checkArgument
import com.google.common.base.Preconditions.checkNotNull
import com.google.common.base.Strings.emptyToNull
import org.apache.commons.lang3.StringUtils.isBlank

class ConfigurationService private constructor(properties: Properties) : Closeable {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val executorService = Executors.newSingleThreadScheduledExecutor()
    private val gson = Gson()
    val instanceId = Math.abs(Random().nextLong())
    private var isDirty = false
    var cache: File? = null
        private set
    var temp: File? = null
        private set
    var database: File? = null
        private set
    private var configuration: File? = null
    val clientVersion: String
    var deviceName: String? = null
        private set
    var deviceId: String
        private set
    var keystoreAlgo: String? = null
        private set
    val repositoryH2Config: String
    private val folders: MutableMap<String, FolderInfo>
    private val peers: MutableMap<String, DeviceInfo>
    var keystore: ByteArray? = null
        private set
    val discoveryServers: List<String>

    val clientName: String
        get() = "syncthing-client"

    val folderNames: Set<String>
        get() = Sets.newTreeSet(folders.keys)

    val peerIds: Set<String>
        get() = Sets.newTreeSet(peers.keys)

    val storageInfo: StorageInfo
        get() = StorageInfo()

    init {
        deviceName = properties.getProperty(DEVICE_NAME)
        if (isBlank(deviceName)) {
            try {
                deviceName = InetAddress.getLocalHost().hostName
            } catch (ex: UnknownHostException) {
                logger.warn("", ex)
            }

            if (isBlank(deviceName) || equal(deviceName, "localhost")) {
                deviceName = "s-client"
            }
        }
        deviceId = properties.getProperty(DEVICE_ID)
        keystoreAlgo = properties.getProperty(KEYSTORE_ALGO)
        folders = Collections.synchronizedMap(Maps.newHashMap())
        val folderValue = properties.getProperty(FOLDERS)
        val folderConfigList = if (isBlank(folderValue)) FolderConfigList() else gson.fromJson(folderValue, FolderConfigList::class.java)
        for (folderConfig in folderConfigList.folders) {
            folders.put(folderConfig.folder!!, FolderInfo(folderConfig.folder!!, folderConfig.label))
        }
        val keystoreValue = properties.getProperty(KEYSTORE)
        if (!Strings.isNullOrEmpty(keystoreValue)) {
            keystore = BaseEncoding.base64().decode(keystoreValue)
        }
        val cacheDir = properties.getProperty(CACHE)
        if (!isBlank(cacheDir)) {
            cache = File(cacheDir)
        } else {
            cache = File(System.getProperty("java.io.tmpdir"), "a_sync_client_cache")
        }
        cache!!.mkdirs()
        checkArgument(cache!!.isDirectory && cache!!.canWrite(), "invalid cache dir = %s", cache)
        val tempDir = properties.getProperty(TEMP)
        if (!isBlank(tempDir)) {
            temp = File(tempDir)
        } else {
            temp = File(System.getProperty("java.io.tmpdir"), "a_sync_client_temp")
        }
        temp!!.mkdirs()
        checkArgument(temp!!.isDirectory && temp!!.canWrite(), "invalid temp dir = %s", temp)
        val dbDir = properties.getProperty(DATABASE)
        if (!isBlank(dbDir)) {
            database = File(dbDir)
        } else {
            database = File(System.getProperty("user.home"), ".config/sclient/db")
        }
        database!!.mkdirs()
        checkArgument(database!!.isDirectory && database!!.canWrite(), "invalid database dir = %s", database)
        peers = Collections.synchronizedMap(Maps.newHashMap())
        val peersValue = properties.getProperty(PEERS)
        val deviceConfigList = if (isBlank(peersValue)) DeviceConfigList() else gson.fromJson(peersValue, DeviceConfigList::class.java)
        for (deviceConfig in deviceConfigList.devices) {
            peers.put(deviceConfig.deviceId!!, DeviceInfo(deviceConfig.deviceId!!, deviceConfig.name))
        }
        val discoveryServerValue = properties.getProperty(DISCOVERY_SERVERS)
        discoveryServers = if (Strings.isNullOrEmpty(discoveryServerValue)) emptyList() else Arrays.asList(*discoveryServerValue.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
        clientVersion = MoreObjects.firstNonNull(emptyToNull(javaClass.`package`.implementationVersion), "0.0.0")// version info from MANIFEST, with 'safe' default fallback
        val configurationValue = properties.getProperty(CONFIGURATION)
        if (!isBlank(configurationValue)) {
            configuration = File(configurationValue)
        }
        repositoryH2Config = properties.getProperty(REPOSITORY_H2_CONFIG)
    }

    @Synchronized private fun export(): Properties {
        val properties = object : Properties() {
            @Synchronized override fun keys(): Enumeration<Any> {
                val list = super.keys().toList() as List<String>
                Collections.sort(list)
                return Collections.enumeration(list)
            }

        }
        if (!isBlank(deviceName)) {
            properties.setProperty(DEVICE_NAME, deviceName)
        }
        if (!isBlank(deviceId)) {
            properties.setProperty(DEVICE_ID, deviceId)
        }
        val folderConfigList = FolderConfigList()
        for (folderInfo in folders.values) {
            val folderConfig = FolderConfig()
            folderConfig.folder = folderInfo.folder
            folderConfig.label = folderInfo.label
            folderConfigList.folders.add(folderConfig)
        }
        properties.setProperty(FOLDERS, gson.toJson(folderConfigList))
        val deviceConfigList = DeviceConfigList()
        for (deviceInfo in peers.values) {
            val deviceConfig = DeviceConfig()
            deviceConfig.deviceId = deviceInfo.deviceId
            deviceConfig.name = deviceInfo.name
            deviceConfigList.devices.add(deviceConfig)
        }
        properties.setProperty(PEERS, gson.toJson(deviceConfigList))
        properties.setProperty(DATABASE, database!!.absolutePath)
        properties.setProperty(TEMP, temp!!.absolutePath)
        properties.setProperty(CACHE, cache!!.absolutePath)
        if (keystore != null) {
            properties.setProperty(KEYSTORE, BaseEncoding.base64().encode(keystore!!))
        }
        if (!isBlank(keystoreAlgo)) {
            properties.setProperty(KEYSTORE_ALGO, keystoreAlgo)
        }
        properties.setProperty(DISCOVERY_SERVERS, Joiner.on(",").join(discoveryServers))
        return properties
    }

    fun getFolders(): List<FolderInfo> {
        return Lists.newArrayList(folders.values)
    }

    fun getPeers(): List<DeviceInfo> {
        return Lists.newArrayList(peers.values)
    }

    override fun close() {
        executorService.shutdown()
        ExecutorUtils.awaitTerminationSafe(executorService)
    }

    inner class StorageInfo {

        val availableSpace: Long
            get() = database!!.freeSpace

        val usedSpace: Long
            get() = FileUtils.sizeOfDirectory(database!!)

        val usedTempSpace: Long
            get() = FileUtils.sizeOfDirectory(cache!!) + FileUtils.sizeOfDirectory(temp!!)

        fun dumpAvailableSpace(): String {
            val stringWriter = StringWriter()
            stringWriter.append("dir / used space / free space")
            stringWriter.append("\n\tcache = ")
                    .append(cache.toString())
                    .append(" ")
                    .append(FileUtils.byteCountToDisplaySize(FileUtils.sizeOfDirectory(cache!!)))
                    .append(" / ")
                    .append(FileUtils.byteCountToDisplaySize(cache!!.freeSpace))
            stringWriter.append("\n\ttemp = ")
                    .append(temp.toString())
                    .append(" ")
                    .append(FileUtils.byteCountToDisplaySize(FileUtils.sizeOfDirectory(temp!!)))
                    .append(" / ")
                    .append(FileUtils.byteCountToDisplaySize(temp!!.freeSpace))
            stringWriter.append("\n\tdatabase = ")
                    .append(database.toString())
                    .append(" ")
                    .append(FileUtils.byteCountToDisplaySize(FileUtils.sizeOfDirectory(database!!)))
                    .append(" / ")
                    .append(FileUtils.byteCountToDisplaySize(database!!.freeSpace))
            return stringWriter.toString()
        }

    }

    fun edit(): Editor {
        return Editor()
    }

    inner class Editor internal constructor() {

        fun setKeystore(keystore: ByteArray?): Editor {
            this@ConfigurationService.keystore = keystore
            return this
        }

        fun setKeystoreAlgo(keystoreAlgo: String?): Editor {
            this@ConfigurationService.keystoreAlgo = keystoreAlgo
            return this
        }

        fun setDeviceName(deviceName: String): Editor {
            this@ConfigurationService.deviceName = deviceName
            return this
        }

        fun setFolders(folderList: Iterable<FolderInfo>): Editor {
            checkNotNull(folderList)
            folders.clear()
            addFolders(folderList)
            return this
        }

        fun addFolders(vararg folders: FolderInfo): Boolean {
            return addFolders(Lists.newArrayList(*folders))
        }

        fun addFolders(newFolders: Iterable<FolderInfo>?): Boolean {
            var added = false
            if (newFolders != null) {
                for (folderInfo in newFolders) {
                    val old = folders.put(folderInfo.folder, folderInfo)
                    if (old == null) {
                        added = true
                    }
                }
            }
            return added
        }

        fun addPeers(vararg peers: DeviceInfo): Boolean {
            return addPeers(Arrays.asList(*peers))
        }

        fun addPeers(peers: Iterable<DeviceInfo>?): Boolean {
            var added = false
            if (peers != null) {
                for (deviceInfo in peers) {
                    val old = this@ConfigurationService.peers.put(deviceInfo.deviceId, deviceInfo)
                    if (old == null) {
                        added = true
                    }
                }
            }
            return added
        }

        fun setPeers(peers: Iterable<DeviceInfo>): Editor {
            this@ConfigurationService.peers.clear()
            addPeers(peers)
            return this
        }

        fun removePeer(deviceId: String): Editor {
            peers.remove(deviceId)
            return this
        }

        fun persistNow() {
            isDirty = true
            storeConfiguration()
        }

        fun persistLater() {
            isDirty = true
            executorService.submit { storeConfiguration() }
        }

        private fun storeConfiguration() {
            if (configuration != null) {
                if (isDirty) {
                    isDirty = false
                    newWriter().writeTo(configuration!!)
                }
            } else {
                logger.debug("dummy save config, no file set")
            }
        }

        fun setDeviceId(deviceId: String): Editor {
            this@ConfigurationService.deviceId = deviceId
            return this
        }
    }

    fun newWriter(): Writer {
        return Writer()
    }

    class Loader {

        private val logger = LoggerFactory.getLogger(javaClass)
        private val customProperties = Properties()

        fun setTemp(temp: File): Loader {
            customProperties.setProperty(TEMP, temp.absolutePath)
            return this
        }

        fun setDatabase(database: File): Loader {
            customProperties.setProperty(DATABASE, database.absolutePath)
            return this
        }

        fun setCache(cache: File): Loader {
            customProperties.setProperty(CACHE, cache.absolutePath)
            return this
        }

        fun loadFrom(file: File?): ConfigurationService {
            val properties = Properties()
            try {
                properties.load(InputStreamReader(javaClass.getResourceAsStream("/default.properties")))
            } catch (ex: IOException) {
                throw RuntimeException(ex)
            }

            if (file != null) {
                if (file.isFile && file.canRead()) {
                    try {
                        properties.load(FileReader(file))
                    } catch (ex: IOException) {
                        logger.error("error loading configuration from file = " + file, ex)
                    }

                }
                properties.put(CONFIGURATION, file.absolutePath)
            }
            properties.putAll(customProperties)
            return ConfigurationService(properties)
        }

        fun load(): ConfigurationService {
            return loadFrom(null)
        }
    }

    inner class Writer internal constructor() {

        fun writeTo(file: File) {
            val properties = export()
            if (!file.exists()) {
                file.parentFile.mkdirs()
            }
            try {
                FileWriter(file).use { fileWriter ->
                    properties.store(fileWriter, null)
                    logger.debug("configuration saved to {}", file)
                }
            } catch (ex: IOException) {
                logger.error("error storing configuration to file = " + file, ex)
            }

        }

        fun dumpToString(): String {
            try {
                val properties = export()
                properties.setProperty("volatile_instanceid", instanceId.toString())
                properties.setProperty("volatile_clientname", clientName)
                properties.setProperty("volatile_clientversion", clientVersion)
                val stringWriter = StringWriter()
                properties.store(stringWriter, null)
                return stringWriter.toString()
            } catch (ex: IOException) {
                throw RuntimeException(ex)
            }

        }

    }

    companion object {

        private val DEVICE_NAME = "devicename"
        private val FOLDERS = "folders"
        private val PEERS = "peers"
        private val INDEX = "index"
        private val DATABASE = "database"
        private val TEMP = "temp"
        private val CACHE = "cache"
        private val KEYSTORE = "keystore"
        private val DEVICE_ID = "deviceid"
        private val KEYSTORE_ALGO = "keystorealgo"
        private val DISCOVERY_SERVERS = "discoveryserver"
        private val CONFIGURATION = "configuration"
        private val REPOSITORY_H2_CONFIG = "repository.h2.dboptions"

        fun newLoader(): Loader {
            return Loader()
        }
    }
}
