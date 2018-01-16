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
package net.syncthing.java.discovery

import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.events.DeviceAddressReceivedEvent
import net.syncthing.java.core.interfaces.DeviceAddressRepository
import net.syncthing.java.core.utils.ExecutorUtils
import net.syncthing.java.discovery.protocol.GlobalDiscoveryHandler
import net.syncthing.java.discovery.protocol.LocalDiscoveryHandler
import net.syncthing.java.discovery.utils.AddressRanker
import org.apache.commons.lang3.tuple.Pair
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.*
import java.util.concurrent.Executors

class DiscoveryHandler(private val configuration: ConfigurationService, private val deviceAddressRepository: DeviceAddressRepository) : Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val globalDiscoveryHandler = GlobalDiscoveryHandler(configuration)
    private val localDiscoveryHandler = LocalDiscoveryHandler(configuration)
    val eventBus = EventBus()
    private val executorService = Executors.newCachedThreadPool()
    private val deviceAddressMap = Collections.synchronizedMap(hashMapOf<Pair<String, String>, DeviceAddress>())
    private var isClosed = false

    private var shouldLoadFromDb = true
    private var shouldLoadFromGlobal = true
    private var shouldStartLocalDiscovery = true

    fun getAllWorkingDeviceAddresses() = deviceAddressMap.values.filter { it.isWorking }

    init {
        logger.info("Initializing discovery handler")
        localDiscoveryHandler.eventBus.register(object : Any() {
            @Subscribe
            fun handleMessageReceivedEvent(event: LocalDiscoveryHandler.MessageReceivedEvent) {
                logger.info("received device address list from local discovery")
                processDeviceAddressBg(event.deviceAddresses)
            }
        })
    }

    private fun updateAddressesBg() {
        if (shouldLoadFromDb) {
            shouldLoadFromDb = false
            executorService.submit {
                val list = this@DiscoveryHandler.deviceAddressRepository.findAllDeviceAddress()
                logger.info("received device address list from database")
                processDeviceAddressBg(list)
            }
        }
        if (shouldStartLocalDiscovery) {
            shouldStartLocalDiscovery = false
            localDiscoveryHandler.startListener()
            localDiscoveryHandler.sendAnnounceMessage()
        }
        if (shouldLoadFromGlobal) {
            shouldLoadFromGlobal = false //TODO timeout for reload
            executorService.submit {
                for (deviceId in this@DiscoveryHandler.configuration.peerIds) {
                    val list = globalDiscoveryHandler.query(deviceId)
                    logger.info("received device address list from global discovery")
                    processDeviceAddressBg(list)
                }
            }
        }
    }

    private fun processDeviceAddressBg(deviceAddresses: Iterable<DeviceAddress>) {
        if (isClosed) {
            logger.debug("discarding device addresses, discovery handler already closed")
        } else {
            executorService.submit {
                logger.info("processing device address list")
                val list = deviceAddresses.toList()
                val peers = configuration.peerIds.toSet()
                //do not process address already processed
                list.filter { deviceAddress ->
                    !peers.contains(deviceAddress.deviceId) || deviceAddressMap.containsKey(Pair.of(deviceAddress.deviceId, deviceAddress.address))
                }
                AddressRanker.testAndRank(list)
                        .forEach { putDeviceAddress(it) }
            }
        }
    }

    private fun putDeviceAddress(deviceAddress: DeviceAddress) {
        logger.info("acquired device address = {}", deviceAddress)
        deviceAddressMap.put(Pair.of(deviceAddress.deviceId, deviceAddress.address), deviceAddress)
        deviceAddressRepository.updateDeviceAddress(deviceAddress)
        eventBus.post(object : DeviceAddressUpdateEvent() {
            override val deviceAddress: DeviceAddress
                get() = deviceAddress
        })
    }

    fun newDeviceAddressSupplier(): DeviceAddressSupplier {
        val deviceAddressSupplier = DeviceAddressSupplier(this)
        updateAddressesBg()
        return deviceAddressSupplier
    }

    override fun close() {
        if (!isClosed) {
            isClosed = true
            localDiscoveryHandler.close()
            globalDiscoveryHandler.close()
            executorService.shutdown()
            ExecutorUtils.awaitTerminationSafe(executorService)
        }
    }

    abstract inner class DeviceAddressUpdateEvent : DeviceAddressReceivedEvent {

        abstract val deviceAddress: DeviceAddress

        override fun getDeviceAddresses(): List<DeviceAddress> {
            return listOf(deviceAddress)
        }
    }

}
