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

import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceId
import net.syncthing.java.core.beans.DeviceStats
import net.syncthing.java.core.beans.DeviceStats.DeviceStatus
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.events.DeviceAddressActiveEvent
import net.syncthing.java.core.utils.ExecutorUtils
import java.io.Closeable
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class DevicesHandler(private val configuration: ConfigurationService) : Closeable {

    private val deviceStatsMap = Collections.synchronizedMap(mutableMapOf<DeviceId, DeviceStats>())
    private val scheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    fun getDeviceStatsList(): Collection<DeviceStats> {
        loadDevicesFromConfiguration()
        return Collections.unmodifiableCollection(deviceStatsMap.values)
    }

    init {
        loadDevicesFromConfiguration()
        scheduledExecutorService.scheduleAtFixedRate({
            for (deviceStats in deviceStatsMap.values) {
                when (deviceStats.status) {
                    DeviceStats.DeviceStatus.ONLINE_ACTIVE -> if (Date().time - deviceStats.lastActive.time > 5 * 1000) {
                        pushDeviceStats(deviceStats.copyBuilder().setStatus(DeviceStatus.ONLINE_INACTIVE).build())
                    }
                }
            }
        }, 5, 5, TimeUnit.SECONDS)
    }

    fun handleDeviceAddressActiveEvent(deviceId: DeviceId) {
        pushDeviceStats(getDeviceStats(deviceId)
                .copyBuilder()
                .setLastActive(Date())
                .setStatus(DeviceStats.DeviceStatus.ONLINE_ACTIVE)
                .build())
    }

    fun handleDeviceAddressReceivedEvent(deviceAddress: DeviceAddress) {
        if (deviceAddress.isWorking()) {
            val deviceStats = getDeviceStats(deviceAddress.deviceId())
            val newStatus: DeviceStatus
            when (deviceStats.status) {
                DeviceStats.DeviceStatus.OFFLINE -> newStatus = DeviceStatus.ONLINE_INACTIVE
                else -> newStatus = deviceStats.status
            }
            pushDeviceStats(deviceStats.copyBuilder().setStatus(newStatus).setLastSeen(Date()).build())
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

    fun getDeviceStats(deviceId: DeviceId): DeviceStats {
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
        deviceStatsMap[deviceStats.deviceId] = deviceStats
    }

    override fun close() {
        scheduledExecutorService.shutdown()
        ExecutorUtils.awaitTerminationSafe(scheduledExecutorService)
    }
}
