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
package net.syncthing.java.discovery.utils

import com.google.common.base.Stopwatch
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Sets
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceAddress.AddressType
import org.apache.commons.lang3.tuple.Pair
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketAddress
import java.util.*
import java.util.concurrent.*
import kotlin.collections.ArrayList

internal class AddressRanker private constructor(sourceAddresses: Iterable<DeviceAddress>) : Closeable {

    companion object {

        fun dumpAddressRanking(list: List<DeviceAddress>): String {
            return list.fold("", {r, t -> r + "${t.deviceId}\t${t.address}\n"})
        }

        private val TCP_CONNECTION_TIMEOUT = 1000
        private val BASE_SCORE_MAP = ImmutableMap.builder<AddressType, Int>()
                .put(AddressType.TCP, 0)
                .put(AddressType.RELAY, TCP_CONNECTION_TIMEOUT * 2)
                .put(AddressType.HTTP_RELAY, TCP_CONNECTION_TIMEOUT * TCP_CONNECTION_TIMEOUT * 2)
                .put(AddressType.HTTPS_RELAY, TCP_CONNECTION_TIMEOUT * TCP_CONNECTION_TIMEOUT * 2)
                .build()
        private val ACCEPTED_ADDRESS_TYPES = BASE_SCORE_MAP.keys

        fun testAndRank(list: Iterable<DeviceAddress>): List<DeviceAddress> {
            try {
                AddressRanker(list).use { addressRanker ->
                    addressRanker.testAndRankAndWait()
                    return addressRanker.getTargetAddresses()
                }
            } catch (ex: InterruptedException) {
                throw RuntimeException(ex)
            }

        }
    }

    private val logger = LoggerFactory.getLogger(javaClass)
    private val executorService = Executors.newCachedThreadPool()
    private val sourceAddresses = sourceAddresses.toList()
    private val targetAddresses = Collections.synchronizedList(ArrayList<DeviceAddress>())

    private val socketAddressScoreCache = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.SECONDS)
            .build(object : CacheLoader<Pair<String, Int>, Int>() {
                @Throws(Exception::class)
                override fun load(key: Pair<String, Int>): Int? {
                    return doTestTcpConnection(InetSocketAddress(InetAddress.getByName(key.left), key.right))
                }
            })

    private fun preprocessDeviceAddresses(list: List<DeviceAddress>): List<DeviceAddress> {
        val res = Sets.newLinkedHashSet<DeviceAddress>()
        for (deviceAddress in list) {
            logger.debug("preprocess address = {}", deviceAddress.address)
            if (deviceAddress.type == AddressType.RELAY && deviceAddress.containsUriParamValue("httpUrl")) {
                val httpUrl = deviceAddress.getUriParam("httpUrl")
                val httpRelayAddress = deviceAddress.copyBuilder()
                        .setAddress("relay-" + httpUrl!!).build()
                logger.debug("extracted http relay address = {}", httpRelayAddress.address)
                res.add(httpRelayAddress)
            }
            res.add(deviceAddress)
        }
        return res.toList()
    }

    @Throws(InterruptedException::class)
    private fun testAndRankAndWait() {
        logger.trace("testing and ranking peer addresses")
        val futures = arrayListOf<Future<DeviceAddress>>()
        for (deviceAddress in preprocessDeviceAddresses(sourceAddresses)) {
            logger.debug("testAndRank({})", deviceAddress)
            futures.add(executorService.submit<DeviceAddress> { testAndRank(deviceAddress) })
        }
        for (future in futures) {
            try {
                val deviceAddress = future.get((TCP_CONNECTION_TIMEOUT * 2).toLong(), TimeUnit.MILLISECONDS)
                if (deviceAddress != null) {
                    targetAddresses.add(deviceAddress)
                }
            } catch (ex: ExecutionException) {
                throw RuntimeException(ex)
            } catch (ex: TimeoutException) {
                logger.warn("test address timeout : {}", ex.toString())
            }

        }
        targetAddresses.sortBy { it.score }
    }

    fun dumpAddressRanking(): String {
        return dumpAddressRanking(targetAddresses)
    }

    private fun testAndRank(deviceAddress: DeviceAddress): DeviceAddress? {
        if (!ACCEPTED_ADDRESS_TYPES.contains(deviceAddress.type)) {
            logger.trace("dropping unsupported address = {}", deviceAddress)
            return null
        }
        val baseScore = BASE_SCORE_MAP.getOrDefault(deviceAddress.type, 0)
        val ping = testTcpConnection(deviceAddress.socketAddress)
        if (ping < 0) {
            logger.trace("dropping unreacheable address = {}", deviceAddress)
            return null
        } else {
            return deviceAddress.copyBuilder().setScore(ping + baseScore).build()
        }
    }

    fun getTargetAddresses(): List<DeviceAddress> {
        return targetAddresses
    }

    //    private boolean hasCompleted() {
    //        return count == sourceAddresses.size();
    //    }
    override fun close() {
        executorService.shutdown()
        try {
            executorService.awaitTermination(2, TimeUnit.SECONDS)
        } catch (ex: InterruptedException) {
            logger.warn("", ex)
        }

    }

    private fun doTestTcpConnection(socketAddress: SocketAddress): Int {
        logger.debug("test tcp connection to address = {}", socketAddress)
        val stopwatch = Stopwatch.createStarted()
        try {
            Socket().use { socket ->
                socket.soTimeout = TCP_CONNECTION_TIMEOUT
                socket.connect(socketAddress, TCP_CONNECTION_TIMEOUT)
            }
        } catch (ex: IOException) {
            logger.debug("address unreacheable = {} ({})", socketAddress, ex.toString())
            logger.trace("address unreacheable", ex)
            return -1
        }

        val time = stopwatch.elapsed(TimeUnit.MILLISECONDS).toInt()
        logger.debug("tcp connection to address = {} is ok, time = {} ms", socketAddress, time)
        return time
    }

    private fun testTcpConnection(socketAddress: InetSocketAddress): Int {
        return socketAddressScoreCache.getUnchecked(Pair.of(socketAddress.address.hostAddress, socketAddress.port))
    }
}
