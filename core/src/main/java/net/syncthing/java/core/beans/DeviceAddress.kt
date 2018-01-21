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
package net.syncthing.java.core.beans

import com.google.common.base.Preconditions
import com.google.common.base.Strings
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Iterables
import org.apache.http.NameValuePair
import org.apache.http.client.utils.URLEncodedUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.URI
import java.net.UnknownHostException
import java.nio.charset.StandardCharsets
import java.util.Date
import java.util.Objects

import com.google.common.base.MoreObjects.firstNonNull
import com.google.common.base.Objects.equal
import com.google.common.base.Strings.emptyToNull

class DeviceAddress private constructor(val deviceId: String, val instanceId: Long?, val address: String, producer: AddressProducer?, score: Int?, lastModified: Date?) {
    val producer: AddressProducer
    val score: Int
    val lastModified: Date

    val inetAddress: InetAddress
        @Throws(UnknownHostException::class)
        get() = InetAddress.getByName(address.replaceFirst("^[^:]+://".toRegex(), "").replaceFirst("(:[0-9]+)?(/.*)?$".toRegex(), ""))

    val port: Int
        get() = if (address.matches("^[a-z]+://[^:]+:([0-9]+).*".toRegex())) {
            Integer.parseInt(address.replaceFirst("^[a-z]+://[^:]+:([0-9]+).*".toRegex(), "$1"))
        } else {
            DEFAULT_PORT_BY_PROTOCOL[type]!!
        }

    val type: AddressType
        get() = if (Strings.isNullOrEmpty(address)) {
            AddressType.NULL
        } else if (address.startsWith("tcp://")) {
            AddressType.TCP
        } else if (address.startsWith("relay://")) {
            AddressType.RELAY
        } else if (address.startsWith("relay-http://")) {
            AddressType.HTTP_RELAY
        } else if (address.startsWith("relay-https://")) {
            AddressType.HTTPS_RELAY
        } else {
            AddressType.OTHER
        }

    val socketAddress: InetSocketAddress
        @Throws(UnknownHostException::class)
        get() = InetSocketAddress(inetAddress, port)

    val isWorking: Boolean
        get() = score < Integer.MAX_VALUE

    val isTcp: Boolean
        get() = equal(type, AddressType.TCP)

    //} catch (Exception ex) {
    //    logger.warn("processing invalid url = {}, ex = {}; stripping params", getAddress(), ex.toString());
    //    return URI.create(getAddress().replaceFirst("^([^/]+://[^/]+)(/.*)?$", "$1"));
    //}
    val uriSafe: URI
        get() = URI.create(address)

    init {
        this.producer = firstNonNull(producer, AddressProducer.UNKNOWN)
        this.score = firstNonNull(score, Integer.MAX_VALUE)
        this.lastModified = firstNonNull(lastModified, Date())
    }

    constructor(deviceId: String, address: String) : this(deviceId, null, address, null, null, null) {}

    fun containsUriParam(key: String): Boolean {
        return getUriParam(key) != null
    }

    fun containsUriParamValue(key: String): Boolean {
        return !Strings.isNullOrEmpty(getUriParam(key))
    }

    fun getUriParam(key: String): String? {
        Preconditions.checkNotNull<String>(emptyToNull(key))
        return URLEncodedUtils.parse(uriSafe, StandardCharsets.UTF_8.name())
                .find { input -> input.getName() == key }
                ?.value
    }

    enum class AddressType {
        TCP, RELAY, OTHER, NULL, HTTP_RELAY, HTTPS_RELAY
    }

    enum class AddressProducer {
        LOCAL_DISCOVERY, GLOBAL_DISCOVERY, UNKNOWN
    }

    override fun toString(): String {
        return "DeviceAddress{deviceId=$deviceId, instanceId=$instanceId, address=$address}"
    }

    override fun hashCode(): Int {
        var hash = 3
        hash = 29 * hash + Objects.hashCode(this.deviceId)
        hash = 29 * hash + Objects.hashCode(this.address)
        return hash
    }

    override fun equals(obj: Any?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null) {
            return false
        }
        if (javaClass != obj.javaClass) {
            return false
        }
        val other = obj as DeviceAddress?
        if (this.deviceId != other!!.deviceId) {
            return false
        }
        return this.address == other.address
    }

    fun copyBuilder(): Builder {
        return Builder(deviceId, instanceId, address, producer, score, lastModified)
    }

    class Builder {

        private var deviceId: String? = null
        private var instanceId: Long? = null
        private var address: String? = null
        private var producer: AddressProducer? = null
        private var score: Int? = null
        private var lastModified: Date? = null

        internal constructor() {}

        internal constructor(deviceId: String, instanceId: Long?, address: String, producer: AddressProducer, score: Int?, lastModified: Date) {
            this.deviceId = deviceId
            this.instanceId = instanceId
            this.address = address
            this.producer = producer
            this.score = score
            this.lastModified = lastModified
        }

        fun getLastModified(): Date? {
            return lastModified
        }

        fun setLastModified(lastModified: Date): Builder {
            this.lastModified = lastModified
            return this
        }

        fun getDeviceId(): String? {
            return deviceId
        }

        fun setDeviceId(deviceId: String): Builder {
            this.deviceId = deviceId
            return this
        }

        fun getInstanceId(): Long? {
            return instanceId
        }

        fun setInstanceId(instanceId: Long?): Builder {
            this.instanceId = instanceId
            return this
        }

        fun getAddress(): String? {
            return address
        }

        fun setAddress(address: String): Builder {
            this.address = address
            return this
        }

        fun getProducer(): AddressProducer? {
            return producer
        }

        fun setProducer(producer: AddressProducer): Builder {
            this.producer = producer
            return this
        }

        fun getScore(): Int? {
            return score
        }

        fun setScore(score: Int?): Builder {
            this.score = score
            return this
        }

        fun build(): DeviceAddress {
            return DeviceAddress(deviceId!!, instanceId, address!!, producer, score, lastModified)
        }
    }

    companion object {

        private val logger = LoggerFactory.getLogger(DeviceAddress::class.java)
        private val DEFAULT_PORT_BY_PROTOCOL = ImmutableMap.builder<AddressType, Int>()
                .put(AddressType.TCP, 22000)
                .put(AddressType.RELAY, 22067)
                .put(AddressType.HTTP_RELAY, 80)
                .put(AddressType.HTTPS_RELAY, 443)
                .build()

        fun newBuilder(): Builder {
            return Builder()
        }
    }
}
