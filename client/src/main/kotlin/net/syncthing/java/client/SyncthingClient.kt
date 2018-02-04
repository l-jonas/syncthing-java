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

import net.syncthing.java.bep.BlockPuller
import net.syncthing.java.bep.BlockPusher
import net.syncthing.java.bep.ConnectionHandler
import net.syncthing.java.bep.IndexHandler
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.java.core.configuration.Configuration
import net.syncthing.java.core.security.KeystoreHandler
import net.syncthing.java.core.utils.awaitTerminationSafe
import net.syncthing.java.discovery.DiscoveryHandler
import net.syncthing.java.repository.repo.SqlRepository
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.util.Collections
import java.util.TreeSet
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.collections.ArrayList

class SyncthingClient(private val configuration: Configuration) : Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)
    val discoveryHandler: DiscoveryHandler
    private val sqlRepository = SqlRepository(configuration.databaseFolder)
    val indexHandler: IndexHandler
    private val connections = Collections.synchronizedSet(TreeSet<ConnectionHandler>(compareBy { it.address.score }))
    private val onConnectionChangedListeners = Collections.synchronizedList(mutableListOf<(DeviceId) -> Unit>())
    private var connectDevicesScheduler = Executors.newSingleThreadScheduledExecutor()

    init {
        indexHandler = IndexHandler(configuration, sqlRepository, sqlRepository)
        discoveryHandler = DiscoveryHandler(configuration, sqlRepository)
        connectDevicesScheduler.scheduleAtFixedRate(this::updateIndexFromPeers, 0, 15, TimeUnit.SECONDS)
    }

    fun clearCacheAndIndex() {
        logger.info("clear cache")
        indexHandler.clearIndex()
        configuration.folders = emptySet()
        configuration.persistLater()
        updateIndexFromPeers()
    }

    fun addOnConnectionChangedListener(listener: (DeviceId) -> Unit) {
        onConnectionChangedListeners.add(listener)
    }

    fun removeOnConnectionChangedListener(listener: (DeviceId) -> Unit) {
        assert(onConnectionChangedListeners.contains(listener))
        onConnectionChangedListeners.remove(listener)
    }

    @Throws(IOException::class, KeystoreHandler.CryptoException::class)
    private fun openConnection(deviceAddress: DeviceAddress): ConnectionHandler {
        logger.debug("openConnection($deviceAddress)")
        val connectionHandler = ConnectionHandler(
                configuration, deviceAddress, indexHandler, { connectionHandler, _ ->
                    connectionHandler.close()
                    openConnection(deviceAddress)
                },
                { connection ->
                    connections.remove(connection)
                    onConnectionChangedListeners.forEach { it(connection.deviceId()) }
                })
        connectionHandler.connect()
        connections.add(connectionHandler)
        onConnectionChangedListeners.forEach { it(connectionHandler.deviceId()) }
        return connectionHandler
    }

    private fun getPeerConnections(listener: (connection: ConnectionHandler) -> Unit, completeListener: () -> Unit) {
        logger.debug("getPeerConnections()")
        connections.forEach { listener(it) }
        discoveryHandler.newDeviceAddressSupplier()
                .takeWhile { it != null }
                .filterNotNull()
                .filterNot { connections.map { it.deviceId() }.contains(it.deviceId()) }
                .forEach {
                    try {
                        listener(openConnection(it))
                    } catch (e: IOException) {
                        logger.warn("error connecting to device = $it", e)
                    } catch (e: KeystoreHandler.CryptoException) {
                        logger.warn("error connecting to device = $it", e)
                    }
                }
        completeListener()
    }

    private fun updateIndexFromPeers() {
        logger.debug("updateIndexFromPeers()")
        getPeerConnections({ connection ->
            try {
                logger.debug("updateIndexFromPeers() ${connection.deviceId()}")
                indexHandler.waitForRemoteIndexAcquired(connection)
            } catch (ex: InterruptedException) {
                logger.warn("exception while waiting for index", ex)
            }
        }, {})
    }

    private fun getConnectionForFolder(folder: String, listener: (connection: ConnectionHandler) -> Unit,
                                       errorListener: () -> Unit) {
        logger.debug("getConnectionForFolder($folder)")
        val isConnected = AtomicBoolean(false)
        getPeerConnections({ connection ->
            logger.debug("getConnectionForFolder($folder) checking ${connection.address}")
            if (connection.hasFolder(folder) && !isConnected.get()) {
                logger.debug("getConnectionForFolder($folder) picked ${connection.address}")
                listener(connection)
                isConnected.set(true)
            }
        }, {
            if (!isConnected.get()) {
                errorListener()
            }
        })
    }

    fun getBlockPuller(folderId: String, listener: (BlockPuller) -> Unit, errorListener: () -> Unit) {
        getConnectionForFolder(folderId, { connection ->
            listener(connection.getBlockPuller())
        }, errorListener)
    }

    fun getBlockPusher(folderId: String, listener: (BlockPusher) -> Unit, errorListener: () -> Unit) {
        getConnectionForFolder(folderId, { connection ->
            listener(connection.getBlockPusher())
        }, errorListener)
    }

    fun getPeerStatus(): List<DeviceInfo> {
        return configuration.peers.map { device ->
            val isConnected = connections.any { it.deviceId() == device.deviceId }
            device.copy(isConnected = isConnected)
        }
    }

    override fun close() {
        connectDevicesScheduler.awaitTerminationSafe()
        discoveryHandler.close()
        // Create copy of list, because it will be modified by handleConnectionClosedEvent(), causing ConcurrentModificationException.
        ArrayList(connections).forEach{it.close()}
        indexHandler.close()
        sqlRepository.close()
        assert(onConnectionChangedListeners.isEmpty())
    }

}
