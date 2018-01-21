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
package net.syncthing.java.client.protocol.rp.beans

import com.google.common.base.Strings

import java.net.InetAddress
import java.net.InetSocketAddress

import com.google.common.base.Preconditions.checkNotNull

class SessionInvitation private constructor(val from: String, val key: String, val address: InetAddress, val port: Int, val isServerSocket: Boolean) {

    val socketAddress: InetSocketAddress
        get() = InetSocketAddress(address, port)

    init {
        checkNotNull<String>(Strings.emptyToNull(from))
        checkNotNull<String>(Strings.emptyToNull(key))
        checkNotNull(address)
    }

    class Builder {

        private var from: String? = null
        private var key: String? = null
        private var address: InetAddress? = null
        private var port: Int = 0
        private var isServerSocket: Boolean = false

        fun getFrom(): String? {
            return from
        }

        fun getKey(): String? {
            return key
        }

        fun getAddress(): InetAddress? {
            return address
        }

        fun getPort(): Int {
            return port
        }

        fun isServerSocket(): Boolean {
            return isServerSocket
        }

        fun setFrom(from: String): Builder {
            this.from = from
            return this
        }

        fun setKey(key: String): Builder {
            this.key = key
            return this
        }

        fun setAddress(address: InetAddress): Builder {
            this.address = address
            return this
        }

        fun setPort(port: Int): Builder {
            this.port = port
            return this
        }

        fun setServerSocket(isServerSocket: Boolean): Builder {
            this.isServerSocket = isServerSocket
            return this
        }

        fun build(): SessionInvitation {
            return SessionInvitation(from!!, key!!, address!!, port, isServerSocket)
        }
    }
}
