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

import com.google.common.collect.Lists
import com.google.common.collect.Sets
import com.google.common.eventbus.Subscribe
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.FileBlocks
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.cache.BlockCache
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.security.KeystoreHandler
import net.syncthing.java.devices.DevicesHandler
import net.syncthing.java.discovery.DeviceAddressSupplier
import net.syncthing.java.discovery.DiscoveryHandler
import net.syncthing.java.repository.repo.SqlRepository
import net.syncthing.java.bep.BlockExchangeConnectionHandler
import net.syncthing.java.bep.BlockPuller
import net.syncthing.java.bep.BlockPuller.FileDownloadObserver
import net.syncthing.java.bep.BlockPusher
import net.syncthing.java.bep.BlockPusher.FileUploadObserver
import net.syncthing.java.bep.IndexHandler
import org.apache.commons.lang3.tuple.Pair
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.Closeable
import java.io.IOException
import java.io.InputStream
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.base.Preconditions.checkNotNull
import net.syncthing.java.core.beans.FolderInfo

class SyncthingClient(private val configuration: ConfigurationService) : Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)
    val discoveryHandler: DiscoveryHandler
    private val sqlRepository = SqlRepository(configuration)
    val indexHandler: IndexHandler
    private val connections = Collections.synchronizedList(Lists.newArrayList<BlockExchangeConnectionHandler>())
    val devicesHandler: DevicesHandler

    init {
        indexHandler = IndexHandler(configuration, sqlRepository, sqlRepository)
        discoveryHandler = DiscoveryHandler(configuration, sqlRepository)
        devicesHandler = DevicesHandler(configuration)
        discoveryHandler.eventBus.register(devicesHandler)
    }

    fun clearCacheAndIndex() {
        logger.info("clear cache")
        indexHandler.clearIndex()
        configuration.edit().setFolders(emptyList<FolderInfo>()).persistLater()
        BlockCache.getBlockCache(configuration).clear()
    }

    @Throws(IOException::class, KeystoreHandler.CryptoException::class)
    private fun openConnection(deviceAddress: DeviceAddress): BlockExchangeConnectionHandler {
        val connectionHandler = BlockExchangeConnectionHandler(configuration, deviceAddress)
        connectionHandler.indexHandler = indexHandler
        connectionHandler.eventBus.register(indexHandler)
        connectionHandler.eventBus.register(devicesHandler)
        val shouldRestartForNewFolder = AtomicBoolean(false)
        connectionHandler.eventBus.register(object : Any() {
            @Subscribe
            fun handleConnectionClosedEvent(event: BlockExchangeConnectionHandler.ConnectionClosedEvent) {
                connections.remove(connectionHandler)
            }

            @Subscribe
            fun handleNewFolderSharedEvent(event: BlockExchangeConnectionHandler.NewFolderSharedEvent) {
                shouldRestartForNewFolder.set(true)
            }
        })
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
    private fun getDeviceConnection(address: DeviceAddress): BlockExchangeConnectionHandler {
        for (c in connections) {
            if (c.address.deviceId == address.deviceId) {
                return c
            }
        }
        return openConnection(address)
    }

    private fun getConnectionForFolder(folder: String, listener: (connection: BlockExchangeConnectionHandler) -> Unit,
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
    private fun getPeerConnections(listener: (connection: BlockExchangeConnectionHandler) -> Unit, completeListener: () -> Unit) {
        Thread {
            val addressesSupplier = discoveryHandler.newDeviceAddressSupplier()
            val connectedDevices = Sets.newHashSet<String>()
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
                            logger.warn("error connecting to device = {}", it, e)
                        } catch (e: KeystoreHandler.CryptoException) {
                            logger.warn("error connecting to device = {}", it, e)
                        }
                    }
            completeListener()
            addressesSupplier.close()
        }.start()
    }

    fun updateIndexFromPeers(listener: (successes: Set<String>, failures: Set<String>) -> Unit) {
        // TODO: if there is already an index update in progress, do nothing
        //       this should probably be handled in IndexHandler
        //       at the moment, this is handled on the Android side
        val indexUpdateComplete = Sets.newHashSet<String>()
        getPeerConnections({ connection ->
            try {
                indexHandler.waitForRemoteIndexAquired(connection)
                indexUpdateComplete.add(connection.deviceId)
            } catch (ex: InterruptedException) {
                logger.warn("exception while waiting for index", ex)
            }
        }, {
            val indexUpdateFailed = Sets.difference(configuration.peerIds, indexUpdateComplete)
            listener(indexUpdateComplete, indexUpdateFailed)
        })
    }

    fun pullFile(fileInfo: FileInfo, listener: (fileDownloadObserver: FileDownloadObserver) -> Unit,
                 errorListener: () -> Unit) {
        getConnectionForFolder(fileInfo.folder, { connection ->
            try {
                val fileInfoAndBlocks = indexHandler.waitForRemoteIndexAquired(connection).getFileInfoAndBlocksByPath(fileInfo.folder, fileInfo.path)
                checkNotNull(fileInfoAndBlocks, "file not found in local index for folder = %s path = %s", fileInfo.folder, fileInfo.path)
                val observer = BlockPuller(configuration, connection, false).pullBlocks(fileInfoAndBlocks!!.value)
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
                val pusher = BlockPusher(configuration, connection, false)
                pusher.withIndexHandler(indexHandler)
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
            val pusher = BlockPusher(configuration, connection, false)
            pusher.withIndexHandler(indexHandler)
            val observer = pusher.pushDir(folder, path)
            listener(observer)
        }, errorListener)
    }

    fun pushDelete(folder: String, path: String, listener: (indexEditObserver: BlockPusher.IndexEditObserver) -> Unit,
                   errorListener: () -> Unit) {
        getConnectionForFolder(folder, { connection ->
            try {
                val pusher = BlockPusher(configuration, connection, false)
                pusher.withIndexHandler(indexHandler)
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