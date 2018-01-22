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

import net.syncthing.java.core.security.KeystoreHandler

class DeviceInfo(val deviceId: String, name: String?) {
    val name: String

    init {
        KeystoreHandler.validateDeviceId(deviceId)
        this.name = if (name.isNullOrBlank()) deviceId.substring(0, 7) else name!!
    }

    override fun toString(): String {
        return "DeviceInfo{deviceId=$deviceId, name=$name}"
    }

}
