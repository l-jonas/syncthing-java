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
package net.syncthing.java.devices

import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.google.common.eventbus.EventBus
import com.google.common.eventbus.Subscribe
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.java.core.beans.DeviceStats
import net.syncthing.java.core.beans.DeviceStats.DeviceStatus
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.events.DeviceAddressActiveEvent
import net.syncthing.java.core.events.DeviceAddressReceivedEvent
import net.syncthing.java.core.utils.ExecutorUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.Closeable
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

import com.google.common.base.Preconditions.checkNotNull
import java.util.Collections.singletonList
import net.syncthing.java.devices.DevicesHandler.DeviceStatsUpdateEvent
import org.apache.http.client.methods.RequestBuilder.post



class DevicesHandler(private val configuration: ConfigurationService) : Closeable {

    private val logger = LoggerFactory.getLogger(DevicesHandler::class.java)
    private val deviceStatsMap = Collections.synchronizedMap(Maps.newHashMap<String, DeviceStats>())
    private val scheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    val eventBus = EventBus()

    val deviceStatsList: Collection<DeviceStats>
        get() {
            loadDevicesFromConfiguration()
            return Collections.unmodifiableCollection(deviceStatsMap.values)
        }

    init {
        checkNotNull(configuration)
        loadDevicesFromConfiguration()
        scheduledExecutorService.scheduleAtFixedRate({
            for (deviceStats in Lists.newArrayList(deviceStatsMap.values)) {
                when (deviceStats.status) {
                    DeviceStats.DeviceStatus.ONLINE_ACTIVE -> if (Date().time - deviceStats.lastActive.time > 5 * 1000) {
                        pushDeviceStats(deviceStats.copyBuilder().setStatus(DeviceStatus.ONLINE_INACTIVE).build())
                    }
                }
            }
        }, 5, 5, TimeUnit.SECONDS)
    }

    @Subscribe
    fun handleDeviceAddressActiveEvent(event: DeviceAddressActiveEvent) {
        pushDeviceStats(getDeviceStats(event.getDeviceAddress().deviceId)
                .copyBuilder()
                .setLastActive(Date())
                .setStatus(DeviceStats.DeviceStatus.ONLINE_ACTIVE)
                .build())
    }

    @Subscribe
    fun handleDeviceAddressReceivedEvent(event: DeviceAddressReceivedEvent) {
        for (deviceAddress in event.getDeviceAddresses()) {
            if (deviceAddress.isWorking) {
                val deviceStats = getDeviceStats(deviceAddress.deviceId)
                val newStatus: DeviceStatus
                when (deviceStats.status) {
                    DeviceStats.DeviceStatus.OFFLINE -> newStatus = DeviceStatus.ONLINE_INACTIVE
                    else -> newStatus = deviceStats.status
                }
                pushDeviceStats(deviceStats.copyBuilder().setStatus(newStatus).setLastSeen(Date()).build())
            }
        }
    }

    fun clear() {
        deviceStatsMap.clear()
    }

    private fun loadDevicesFromConfiguration() {
        for (deviceInfo in configuration.getPeers()) {
            if (!deviceStatsMap.containsKey(deviceInfo.deviceId)) {
                pushDeviceStats(DeviceStats.newBuilder().setDeviceId(deviceInfo.deviceId).setName(deviceInfo.name).build())
            }
        }
    }

    fun getDeviceStats(deviceId: String): DeviceStats {
        loadDevicesFromConfiguration()
        if (deviceStatsMap.containsKey(deviceId)) {
            return deviceStatsMap[deviceId]!!
        } else {
            val deviceStats = DeviceStats.newBuilder().setDeviceId(deviceId).build()
            pushDeviceStats(deviceStats)
            return deviceStats
        }
    }

    private fun pushDeviceStats(deviceStats: DeviceStats) {
        deviceStatsMap.put(deviceStats.deviceId, deviceStats)
        val event: DeviceStatsUpdateEvent = object : DeviceStatsUpdateEvent {
            override fun changedDeviceStats() = Collections.singletonList(deviceStats)
        }
        eventBus.post(event)
    }

    interface DeviceStatsUpdateEvent {
        fun changedDeviceStats(): List<DeviceStats>
    }

    override fun close() {
        scheduledExecutorService.shutdown()
        ExecutorUtils.awaitTerminationSafe(scheduledExecutorService)
    }
}
