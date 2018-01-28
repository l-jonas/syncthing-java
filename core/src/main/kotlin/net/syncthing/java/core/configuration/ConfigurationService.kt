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

import com.google.gson.Gson
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.java.core.beans.FolderInfo
import net.syncthing.java.core.configuration.gsonbeans.DeviceConfig
import net.syncthing.java.core.configuration.gsonbeans.DeviceConfigList
import net.syncthing.java.core.configuration.gsonbeans.FolderConfig
import net.syncthing.java.core.configuration.gsonbeans.FolderConfigList
import net.syncthing.java.core.utils.awaitTerminationSafe
import net.syncthing.java.core.utils.submitLogging
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils.isBlank
import org.bouncycastle.util.encoders.Base64
import org.bouncycastle.util.encoders.Hex
import org.slf4j.LoggerFactory
import java.io.*
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.*
import java.util.concurrent.Executors

class ConfigurationService private constructor(properties: Properties) : Closeable {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val executorService = Executors.newSingleThreadScheduledExecutor()
    private val gson = Gson()
    val instanceId = Math.abs(Random().nextLong())
    private var isDirty = false
    val cache: File
    var temp: File
    var database: File
    private var configuration: File? = null
    val clientVersion: String
    var deviceName: String?
        private set
    var deviceId: DeviceId?
        private set
    var keystoreAlgo: String?
        private set
    val repositoryH2Config: String?
    private val folders: MutableMap<String, FolderInfo>
    private val peers: MutableMap<DeviceId, DeviceInfo>
    var keystore: ByteArray? = null
        private set
    val discoveryServers: List<String>

    fun getClientName(): String = "syncthing-client"

    fun getFolderNames(): Set<String> = folders.keys.toSet()

    fun getPeerIds(): Set<DeviceId> = peers.keys.toSet()

    fun getStorageInfo(): StorageInfo = StorageInfo()

    private fun Properties.getPropertySafe(key: String): String? = getProperty(key)

    init {
        deviceName = properties.getPropertySafe(DEVICE_NAME)
        if (isBlank(deviceName)) {
            try {
                deviceName = InetAddress.getLocalHost().hostName
            } catch (ex: UnknownHostException) {
                logger.warn("", ex)
            }

            if (isBlank(deviceName) || deviceName == "localhost") {
                deviceName = "s-client"
            }
        }
        deviceId = properties.getPropertySafe(DEVICE_ID)?.let { DeviceId(it) }
        keystoreAlgo = properties.getPropertySafe(KEYSTORE_ALGO)
        folders = Collections.synchronizedMap(mutableMapOf())
        val folderValue: String? = properties.getPropertySafe(FOLDERS)
        val folderConfigList = if (isBlank(folderValue)) FolderConfigList() else gson.fromJson(folderValue, FolderConfigList::class.java)
        for (folderConfig in folderConfigList.folders) {
            folders.put(folderConfig.folder, FolderInfo(folderConfig.folder, folderConfig.label))
        }
        val keystoreValue = properties.getPropertySafe(KEYSTORE)
        if (keystoreValue != null && !keystoreValue.isEmpty()) {
            keystore = Base64.decode(keystoreValue)
        }
        val cacheDir = properties.getPropertySafe(CACHE)
        cache = if (!isBlank(cacheDir)) {
            File(cacheDir)
        } else {
            File(System.getProperty("java.io.tmpdir"), "a_sync_client_cache")
        }
        cache.mkdirs()
        assert(cache.isDirectory && cache.canWrite(), {"invalid cache dir = $cache"})
        val tempDir = properties.getPropertySafe(TEMP)
        temp = if (!isBlank(tempDir)) {
            File(tempDir)
        } else {
            File(System.getProperty("java.io.tmpdir"), "a_sync_client_temp")
        }
        temp.mkdirs()
        assert(temp.isDirectory && temp.canWrite(), {"invalid temp dir = $temp"})
        val dbDir = properties.getPropertySafe(DATABASE)
        database = if (!isBlank(dbDir)) {
            File(dbDir)
        } else {
            File(System.getProperty("user.home"), ".config/sclient/db")
        }
        database.mkdirs()
        assert(database.isDirectory && database.canWrite(), {"invalid database dir = $database"})
        peers = Collections.synchronizedMap(mutableMapOf())
        val peersValue = properties.getPropertySafe(PEERS)
        val deviceConfigList = if (isBlank(peersValue)) DeviceConfigList() else gson.fromJson(peersValue, DeviceConfigList::class.java)
        for (deviceConfig in deviceConfigList.devices) {
            peers.put(deviceConfig.deviceId, DeviceInfo(deviceConfig.deviceId, deviceConfig.name))
        }
        val discoveryServerValue: String? = properties.getPropertySafe(DISCOVERY_SERVERS)
        discoveryServers = if (discoveryServerValue == null || discoveryServerValue.isEmpty()) emptyList()
                           else discoveryServerValue.split(",".toRegex()).dropLastWhile { it.isEmpty() }
        clientVersion = javaClass.`package`.implementationVersion ?: "0.0.0"// version info from MANIFEST, with 'safe' default fallback
        val configurationValue = properties.getPropertySafe(CONFIGURATION)
        if (!isBlank(configurationValue)) {
            configuration = File(configurationValue)
        }
        repositoryH2Config = properties.getPropertySafe(REPOSITORY_H2_CONFIG)
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
        deviceId?.let {
            properties.setProperty(DEVICE_ID, it.deviceId)
        }
        val folderConfigList = FolderConfigList()
        for (folderInfo in folders.values) {
            val folderConfig = FolderConfig(folderInfo.folder, folderInfo.label)
            folderConfigList.folders.add(folderConfig)
        }
        properties.setProperty(FOLDERS, gson.toJson(folderConfigList))
        val deviceConfigList = DeviceConfigList()
        peers.values
                .map { DeviceConfig(it.deviceId, it.name) }
                .forEach { deviceConfigList.devices.add(it) }
        properties.setProperty(PEERS, gson.toJson(deviceConfigList))
        properties.setProperty(DATABASE, database.absolutePath)
        properties.setProperty(TEMP, temp.absolutePath)
        properties.setProperty(CACHE, cache.absolutePath)
        if (keystore != null) {
            Hex.encode(keystore)
            properties.setProperty(KEYSTORE, Base64.toBase64String(keystore))
        }
        if (!isBlank(keystoreAlgo)) {
            properties.setProperty(KEYSTORE_ALGO, keystoreAlgo)
        }
        properties.setProperty(DISCOVERY_SERVERS, discoveryServers.joinToString(","))
        return properties
    }

    fun getFolders(): List<FolderInfo> = folders.values.toList()

    fun getPeers(): List<DeviceInfo> = peers.values.toList()

    override fun close() {
        executorService.shutdown()
        executorService.awaitTerminationSafe()
    }

    inner class StorageInfo {

        fun dumpAvailableSpace(): String {
            val stringWriter = StringWriter()
            stringWriter.append("dir / used space / free space")
            stringWriter.append("\n\tcache = ")
                    .append(cache.toString())
                    .append(" ")
                    .append(FileUtils.byteCountToDisplaySize(FileUtils.sizeOfDirectory(cache)))
                    .append(" / ")
                    .append(FileUtils.byteCountToDisplaySize(cache.freeSpace))
            stringWriter.append("\n\ttemp = ")
                    .append(temp.toString())
                    .append(" ")
                    .append(FileUtils.byteCountToDisplaySize(FileUtils.sizeOfDirectory(temp)))
                    .append(" / ")
                    .append(FileUtils.byteCountToDisplaySize(temp.freeSpace))
            stringWriter.append("\n\tdatabase = ")
                    .append(database.toString())
                    .append(" ")
                    .append(FileUtils.byteCountToDisplaySize(FileUtils.sizeOfDirectory(database)))
                    .append(" / ")
                    .append(FileUtils.byteCountToDisplaySize(database.freeSpace))
            return stringWriter.toString()
        }

    }

    inner class Editor {

        fun setKeystore(keystore: ByteArray): Editor {
            this@ConfigurationService.keystore = keystore
            return this
        }

        fun setKeystoreAlgo(keystoreAlgo: String): Editor {
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

        fun addFolders(newFolders: Iterable<FolderInfo>): Boolean {
            return newFolders
                    .map { folders.put(it.folder, it) }
                    .contains(null)
        }

        fun addPeers(vararg peers: DeviceInfo): Boolean {
            return addPeers(Arrays.asList(*peers))
        }

        fun addPeers(peers: Iterable<DeviceInfo>): Boolean {
            return peers
                    .map { this@ConfigurationService.peers.put(it.deviceId, it) }
                    .contains(null)
        }

        fun setPeers(peers: Iterable<DeviceInfo>): Editor {
            this@ConfigurationService.peers.clear()
            addPeers(peers)
            return this
        }

        fun removePeer(deviceId: DeviceId): Editor {
            peers.remove(deviceId)
            return this
        }

        fun persistNow() {
            isDirty = true
            storeConfiguration()
        }

        fun persistLater() {
            isDirty = true
            executorService.submitLogging { storeConfiguration() }
        }

        private fun storeConfiguration() {
            configuration?.let {
                if (isDirty) {
                    isDirty = false
                    Writer().writeTo(configuration!!)
                }
            } ?: logger.debug("dummy save config, no file set")
        }

        fun setDeviceId(deviceId: DeviceId): Editor {
            this@ConfigurationService.deviceId = deviceId
            return this
        }
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

        @Throws(IOException::class)
        fun loadFrom(file: File?): ConfigurationService {
            val properties = Properties()
            properties.load(InputStreamReader(javaClass.getResourceAsStream("/default.properties")))

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

    inner class Writer {

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

        @Throws(IOException::class)
        fun dumpToString(): String {
                val properties = export()
                properties.setProperty("volatile_instanceid", instanceId.toString())
                properties.setProperty("volatile_clientname", getClientName())
                properties.setProperty("volatile_clientversion", clientVersion)
                val stringWriter = StringWriter()
                properties.store(stringWriter, null)
                return stringWriter.toString()
        }
    }

    companion object {

        private const val DEVICE_NAME = "devicename"
        private const val FOLDERS = "folders"
        private const val PEERS = "peers"
        private const val INDEX = "index"
        private const val DATABASE = "database"
        private const val TEMP = "temp"
        private const val CACHE = "cache"
        private const val KEYSTORE = "keystore"
        private const val DEVICE_ID = "deviceid"
        private const val KEYSTORE_ALGO = "keystorealgo"
        private const val DISCOVERY_SERVERS = "discoveryserver"
        private const val CONFIGURATION = "configuration"
        private const val REPOSITORY_H2_CONFIG = "repository.h2.dboptions"
    }
}
