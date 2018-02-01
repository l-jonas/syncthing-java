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
package net.syncthing.java.client

import net.syncthing.java.bep.ConnectionHandler
import net.syncthing.java.bep.BlockPuller.FileDownloadObserver
import net.syncthing.java.bep.BlockPusher
import net.syncthing.java.bep.BlockPusher.FileUploadObserver
import net.syncthing.java.bep.IndexHandler
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.cache.BlockCache
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.security.KeystoreHandler
import net.syncthing.java.devices.DevicesHandler
import net.syncthing.java.discovery.DiscoveryHandler
import net.syncthing.java.repository.repo.SqlRepository
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.io.InputStream
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.collections.ArrayList

class SyncthingClient(private val configuration: Configuration) : Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)
    val discoveryHandler: DiscoveryHandler
    private val sqlRepository = SqlRepository(configuration.databaseFolder)
    val indexHandler: IndexHandler
    private val connections = Collections.synchronizedList(mutableListOf<ConnectionHandler>())
    val devicesHandler: DevicesHandler

    init {
        indexHandler = IndexHandler(configuration, sqlRepository, sqlRepository)
        devicesHandler = DevicesHandler(configuration)
        discoveryHandler = DiscoveryHandler(configuration, sqlRepository, devicesHandler::handleDeviceAddressReceivedEvent)
    }

    fun clearCacheAndIndex() {
        logger.info("clear cache")
        indexHandler.clearIndex()
        configuration.folders = emptySet()
        configuration.persistLater()
        BlockCache.getBlockCache(configuration).clear()
    }

    @Throws(IOException::class, KeystoreHandler.CryptoException::class)
    private fun openConnection(deviceAddress: DeviceAddress): ConnectionHandler {
        val shouldRestartForNewFolder = AtomicBoolean(false)
        val connectionHandler = ConnectionHandler(
                configuration, deviceAddress, indexHandler, devicesHandler::handleDeviceAddressActiveEvent,
                { shouldRestartForNewFolder.set(true) },
                { connections.remove(it)})
        connectionHandler.connect()
        connections.add(connectionHandler)
        return if (shouldRestartForNewFolder.get()) {
                logger.info("restart connection for new folder shared")
                connectionHandler.close()
                openConnection(deviceAddress)
            } else {
                connectionHandler
            }
    }

    @Throws(IOException::class, KeystoreHandler.CryptoException::class)
    private fun getDeviceConnection(address: DeviceAddress): ConnectionHandler {
        for (c in connections) {
            if (c.address.deviceId == address.deviceId) {
                return c
            }
        }
        return openConnection(address)
    }

    private fun getConnectionForFolder(folder: String, listener: (connection: ConnectionHandler) -> Unit,
                                       errorListener: () -> Unit) {
        val isConnected = AtomicBoolean(false)
        getPeerConnections({ connection ->
            if (connection.hasFolder(folder)) {
                listener(connection)
                isConnected.set(true)
            } else {
                connection.close()
            }
        }, {
            if (!isConnected.get()) {
                errorListener()
            }
        })
    }

    // TODO: should get rid of this and just have getDeviceConnection() connect to single device
    private fun getPeerConnections(listener: (connection: ConnectionHandler) -> Unit, completeListener: () -> Unit) {
        Thread {
            val addressesSupplier = discoveryHandler.newDeviceAddressSupplier()
            val connectedDevices = mutableSetOf<String>()
            addressesSupplier
                    .takeWhile { it != null }
                    .filterNotNull()
                    .filterNot { connectedDevices.contains(it.deviceId) }
                    .forEach {
                        try {
                            val connection = getDeviceConnection(it)
                            connectedDevices.add(it.deviceId)
                            listener(connection)
                        } catch (e: IOException) {
                            logger.warn("error connecting to device = $it", e)
                        } catch (e: KeystoreHandler.CryptoException) {
                            logger.warn("error connecting to device = $it", e)
                        }
                    }
            completeListener()
        }.start()
    }

    fun updateIndexFromPeers(listener: (successes: Set<DeviceId>, failures: Set<DeviceId>) -> Unit) {
        // TODO: if there is already an index update in progress, do nothing
        //       this should probably be handled in IndexHandler
        //       at the moment, this is handled on the Android side
        val indexUpdateComplete = mutableSetOf<DeviceId>()
        getPeerConnections({ connection ->
            try {
                indexHandler.waitForRemoteIndexAquired(connection)
                indexUpdateComplete.add(connection.deviceId())
            } catch (ex: InterruptedException) {
                logger.warn("exception while waiting for index", ex)
            }
        }, {
            val indexUpdateFailed = configuration.peerIds - indexUpdateComplete
            listener(indexUpdateComplete, indexUpdateFailed)
        })
    }

    fun pullFile(fileInfo: FileInfo, listener: (fileDownloadObserver: FileDownloadObserver) -> Unit,
                 errorListener: () -> Unit) {
        getConnectionForFolder(fileInfo.folder, { connection ->
            try {
                val fileInfoAndBlocks = indexHandler.waitForRemoteIndexAquired(connection).getFileInfoAndBlocksByPath(fileInfo.folder, fileInfo.path)
                        ?: error("file not found in local index for folder = ${fileInfo.folder} path = ${fileInfo.path}")
                val observer = connection.getBlockPuller().pullBlocks(fileInfoAndBlocks.value)
                listener(observer)
            } catch (e: InterruptedException) {
                logger.warn("Failed to pull file", e)
                errorListener()
            }
        }, errorListener)
    }

    fun pushFile(data: InputStream, folder: String, path: String, listener: (fileUploadObserver: FileUploadObserver) -> Unit,
                 errorListener: () -> Unit) {
        getConnectionForFolder(folder, { connection ->
            try {
                val pusher = connection.getBlockPusher()
                val fileInfo = indexHandler.waitForRemoteIndexAquired(connection).getFileInfoByPath(folder, path)
                val observer = pusher.pushFile(data, fileInfo, folder, path)
                listener(observer)
            } catch (e: InterruptedException) {
                logger.warn("Failed to pull file", e)
                errorListener()
            }
        }, errorListener)
    }

    fun pushDir(folder: String, path: String, listener: (indexEditObserver: BlockPusher.IndexEditObserver) -> Unit,
                errorListener: () -> Unit) {
        getConnectionForFolder(folder, { connection ->
            val pusher = connection.getBlockPusher()
            val observer = pusher.pushDir(folder, path)
            listener(observer)
        }, errorListener)
    }

    fun pushDelete(folder: String, path: String, listener: (indexEditObserver: BlockPusher.IndexEditObserver) -> Unit,
                   errorListener: () -> Unit) {
        getConnectionForFolder(folder, { connection ->
            try {
                val pusher = connection.getBlockPusher()
                val fileInfo = indexHandler.waitForRemoteIndexAquired(connection).getFileInfoByPath(folder, path)
                val observer = pusher.pushDelete(fileInfo!!, folder, path)
                listener(observer)
            } catch (e: InterruptedException) {
                logger.warn("Failed to push delete", e)
                errorListener()
            }
        }, errorListener)
    }

    override fun close() {
        devicesHandler.close()
        discoveryHandler.close()
        // Create copy of list, because it will be modified by handleConnectionClosedEvent(), causing ConcurrentModificationException.
        ArrayList(connections).forEach{it.close()}
        indexHandler.close()
        sqlRepository.close()
    }

}
