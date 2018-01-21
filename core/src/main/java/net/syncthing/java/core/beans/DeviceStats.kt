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
package net.syncthing.java.core.beans

import java.util.Date

import com.google.common.base.Preconditions.checkNotNull
import com.google.common.base.Strings.emptyToNull
import org.apache.commons.lang3.StringUtils.isBlank

class DeviceStats private constructor(val deviceId: String, name: String, val lastActive: Date, val lastSeen: Date, val status: DeviceStatus) {
    val name: String

    init {
        checkNotNull<String>(emptyToNull(deviceId))
        checkNotNull(status)
        checkNotNull(lastActive)
        checkNotNull(lastSeen)
        this.name = if (isBlank(name)) deviceId.substring(0, 7) else name
    }

    fun copyBuilder(): Builder {
        return Builder(deviceId, name, lastActive, lastSeen, status)
    }

    override fun toString(): String {
        return "DeviceStats{deviceId=$deviceId, name=$name, status=$status}"
    }

    enum class DeviceStatus {
        OFFLINE, ONLINE_ACTIVE, ONLINE_INACTIVE
    }

    class Builder {

        private var deviceId: String? = null
        private var name: String? = null

        private var lastActive = Date(0)
        private var lastSeen = Date(0)

        private var status = DeviceStatus.OFFLINE

        internal constructor() {}

        internal constructor(deviceId: String, name: String, lastActive: Date, lastSeen: Date, status: DeviceStatus) {
            this.deviceId = deviceId
            this.name = name
            this.lastActive = lastActive
            this.lastSeen = lastSeen
            this.status = status
        }

        fun getDeviceId(): String? {
            return deviceId
        }

        fun setDeviceId(deviceId: String): Builder {
            this.deviceId = deviceId
            return this
        }

        fun getName(): String? {
            return name
        }

        fun setName(name: String): Builder {
            this.name = name
            return this
        }

        fun getLastActive(): Date {
            return lastActive
        }

        fun setLastActive(lastActive: Date): Builder {
            this.lastActive = lastActive
            return this
        }

        fun getLastSeen(): Date {
            return lastSeen
        }

        fun setLastSeen(lastSeen: Date): Builder {
            this.lastSeen = lastSeen
            return this
        }

        fun getStatus(): DeviceStatus {
            return status
        }

        fun setStatus(status: DeviceStatus): Builder {
            this.status = status
            return this
        }

        fun build(): DeviceStats {
            return DeviceStats(deviceId!!, name!!, lastActive, lastSeen, status)
        }

    }

    companion object {

        fun newBuilder(): Builder {
            return Builder()
        }
    }
}
