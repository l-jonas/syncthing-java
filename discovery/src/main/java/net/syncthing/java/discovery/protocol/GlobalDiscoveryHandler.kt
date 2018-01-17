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
package net.syncthing.java.discovery.protocol

import com.google.gson.Gson
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.discovery.utils.AddressRanker
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpGet
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.conn.ssl.SSLContextBuilder
import org.apache.http.conn.ssl.TrustSelfSignedStrategy
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.security.KeyManagementException
import java.security.KeyStoreException
import java.security.NoSuchAlgorithmException

internal class GlobalDiscoveryHandler(private val configuration: ConfigurationService) : Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val gson = Gson()

    fun query(deviceId: String, callback: (List<DeviceAddress>) -> Unit) {
        val addresses = pickAnnounceServers()
                .map {
                    try {
                        queryAnnounceServer(it, deviceId)
                    } catch (e: IOException) {
                        logger.warn("Failed to query $it", e)
                        listOf<DeviceAddress>()
                    }
                }
                .flatten()
        logger.info("Discovered addresses for $deviceId: $addresses")
        callback(addresses)
    }

    private fun pickAnnounceServers(): List<String> {
        logger.debug("ranking discovery server addresses")
        val list = AddressRanker
                .pingAddresses(configuration.discoveryServers.map { DeviceAddress(it, "tcp://$it:443") })
        logger.info("discovery server addresses = {}", list.map { it.address })
        return list.mapNotNull { it.deviceId }
    }

    @Throws(IOException::class)
    private fun queryAnnounceServer(server: String, deviceId: String): List<DeviceAddress> {
        try {
            logger.trace("querying server {} for device id {}", server, deviceId)
            val httpClient = HttpClients.custom()
                    .setSSLSocketFactory(SSLConnectionSocketFactory(SSLContextBuilder().loadTrustMaterial(null, TrustSelfSignedStrategy()).build(), SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER))
                    .build()
            val httpGet = HttpGet("https://$server/v2/?device=$deviceId")
            return httpClient.execute<List<DeviceAddress>>(httpGet) { response ->
                when (response.statusLine.statusCode) {
                    HttpStatus.SC_NOT_FOUND -> {
                        logger.debug("device not found: {}", deviceId)
                        return@execute emptyList()
                    }
                    HttpStatus.SC_OK -> {
                        val announcementMessageBean = gson.fromJson(EntityUtils.toString(response.entity), AnnouncementMessageBean::class.java)
                        val list = (announcementMessageBean.addresses ?: emptyList())
                                .map { DeviceAddress(deviceId, it) }
                        logger.debug("found address list = {}", list)
                        return@execute list
                    }
                    else -> throw IOException("http error " + response.statusLine)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is IOException, is NoSuchAlgorithmException, is KeyStoreException, is KeyManagementException ->
                    throw IOException(e)
                else -> throw e
            }
        }
    }

    override fun close() {}

    class AnnouncementMessageBean {
        var addresses: List<String>? = null
    }
}
