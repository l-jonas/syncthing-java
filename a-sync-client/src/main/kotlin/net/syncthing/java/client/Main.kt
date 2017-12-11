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

import com.google.common.collect.Lists
import net.syncthing.java.core.beans.DeviceAddress
import net.syncthing.java.core.beans.DeviceInfo
import net.syncthing.java.core.beans.FileInfo
import net.syncthing.java.core.configuration.ConfigurationService
import net.syncthing.java.core.security.KeystoreHandler
import net.syncthing.java.discovery.DeviceAddressSupplier
import net.syncthing.java.discovery.protocol.GlobalDiscoveryHandler
import net.syncthing.java.discovery.protocol.LocalDiscoveryHandler
import net.syncthing.java.bep.IndexBrowser
import net.syncthing.java.bep.IndexFinder
import org.apache.commons.cli.*
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.util.Arrays
import java.util.Collections
import java.util.concurrent.CountDownLatch

import com.google.common.base.Preconditions.checkArgument

object Main {

    private val logger = LoggerFactory.getLogger(Main::class.java)

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val options = Options()
        options.addOption("C", "set-config", true, "set config file for s-client")
        options.addOption("c", "config", false, "dump config")
        options.addOption("sp", "set-peers", true, "set peer, or comma-separated list of peers")
        options.addOption("q", "query", true, "query directory server for device id")
        options.addOption("d", "discovery", true, "discovery local network for device id")
        options.addOption("p", "pull", true, "pull file from network")
        options.addOption("P", "push", true, "push file to network")
        options.addOption("o", "output", true, "set output file/directory")
        options.addOption("i", "input", true, "set input file/directory")
        options.addOption("lp", "list-peers", false, "list peer addresses")
        options.addOption("a", "address", true, "use this peer addresses")
        options.addOption("L", "list-remote", false, "list folder (root) content from network")
        options.addOption("I", "list-info", false, "dump folder info from network")
        options.addOption("li", "list-info", false, "list folder info from local db")
        options.addOption("s", "search", true, "search local index for <term>")
        options.addOption("D", "delete", true, "push delete to network")
        options.addOption("M", "mkdir", true, "push directory create to network")
        options.addOption("h", "help", false, "print help")
        val parser = DefaultParser()
        val cmd = parser.parse(options, args)

        if (cmd.hasOption("h")) {
            val formatter = HelpFormatter()
            formatter.printHelp("s-client", options)
            return
        }

        val configFile = if (cmd.hasOption("C")) File(cmd.getOptionValue("C")) else File(System.getProperty("user.home"), ".s-client.properties")
        logger.info("using config file = {}", configFile)
        val configuration = ConfigurationService.newLoader().loadFrom(configFile)
        FileUtils.cleanDirectory(configuration.temp)
        KeystoreHandler.newLoader().loadAndStore(configuration)
        val syncthingClient = SyncthingClient(configuration)
        if (cmd.hasOption("c")) {
            logger.info("configuration =\n{}", configuration.newWriter().dumpToString())
        } else {
            logger.trace("configuration =\n{}", configuration.newWriter().dumpToString())
        }
        logger.debug("{}", configuration.storageInfo.dumpAvailableSpace())

        if (cmd.hasOption("sp")) {
            val peers = cmd.getOptionValue("sp")
                    .split(",")
                    .map { it.trim() }
                    .filterNot { it.isEmpty() }
                    .toList()
            logger.info("set peers = {}", peers)
            configuration.edit().setPeers(emptyList<DeviceInfo>())
            for (peer in peers) {
                KeystoreHandler.validateDeviceId(peer)
                configuration.edit().addPeers(DeviceInfo(peer, null))
            }
            configuration.edit().persistNow()
        }

        if (cmd.hasOption("q")) {
            val deviceId = cmd.getOptionValue("q")
            logger.info("query device id = {}", deviceId)
            val deviceAddresses = GlobalDiscoveryHandler(configuration).query(deviceId)
            logger.info("server response = {}", deviceAddresses)
        }
        if (cmd.hasOption("d")) {
            val deviceId = cmd.getOptionValue("d")
            logger.info("discovery device id = {}", deviceId)
            val deviceAddresses = LocalDiscoveryHandler(configuration).queryAndClose(deviceId)
            logger.info("local response = {}", deviceAddresses)
        }

        if (cmd.hasOption("p")) {
            val folderAndPath = cmd.getOptionValue("p")
            logger.info("file path = {}", folderAndPath)
            val folder = folderAndPath.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[0]
            val path = folderAndPath.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[1]
            val latch = CountDownLatch(1)
            syncthingClient.pullFile(folder, path, { observer ->
                try {
                    val inputStream = observer.waitForComplete().inputStream
                    val fileName = syncthingClient.indexHandler.getFileInfoByPath(folder, path)!!.fileName
                    val file: File
                    if (cmd.hasOption("o")) {
                        val param = File(cmd.getOptionValue("o"))
                        file = if (param.isDirectory) File(param, fileName) else param
                    } else {
                        file = File(fileName)
                    }
                    FileUtils.copyInputStreamToFile(inputStream, file)
                    logger.info("saved file to = {}", file.absolutePath)
                } catch (e: InterruptedException) {
                    logger.warn("", e)
                } catch (e: IOException) {
                    logger.warn("", e)
                }
            }) { logger.warn("Failed to pull file") }
            latch.await()
        }
        if (cmd.hasOption("P")) {
            var path = cmd.getOptionValue("P")
            val file = File(cmd.getOptionValue("i"))
            checkArgument(!path.startsWith("/")) //TODO check path syntax
            logger.info("file path = {}", path)
            val folder = path.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[0]
            path = path.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[1]
            val latch = CountDownLatch(1)
            syncthingClient.pushFile(FileInputStream(file), folder, path, { fileUploadObserver ->
                while (!fileUploadObserver.isCompleted) {
                    try {
                        fileUploadObserver.waitForProgressUpdate()
                    } catch (e: InterruptedException) {
                        logger.warn("", e)
                    }

                    logger.debug("upload progress {}", fileUploadObserver.progressMessage)
                }
                latch.countDown()
            }) { logger.warn("Failed to upload file") }
            latch.await()
            logger.info("uploaded file to network")
        }
        if (cmd.hasOption("D")) {
            var path = cmd.getOptionValue("D")
            val folder = path.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[0]
            path = path.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[1]
            logger.info("delete path = {}", path)
            val latch = CountDownLatch(1)
            syncthingClient.pushDelete(folder, path, { observer ->
                try {
                    observer.waitForComplete()
                } catch (e: InterruptedException) {
                    logger.warn("", e)
                }

                latch.countDown()
            }) { logger.warn("Failed to delete path") }
            latch.await()
            logger.info("deleted path")
        }
        if (cmd.hasOption("M")) {
            var path = cmd.getOptionValue("M")
            val folder = path.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[0]
            path = path.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[1]
            logger.info("dir path = {}", path)
            val latch = CountDownLatch(1)
            syncthingClient.pushDir(folder, path, { observer ->
                try {
                    observer.waitForComplete()
                } catch (e: InterruptedException) {
                    logger.warn("", e)
                }

                latch.countDown()
            }) { logger.warn("Failed to push directory") }
            latch.await()
            logger.info("uploaded dir to network")
        }
        if (cmd.hasOption("L")) {
            updateIndex(syncthingClient)
            for (folder in syncthingClient.indexHandler.folderList) {
                syncthingClient.indexHandler.newIndexBrowserBuilder().setFolder(folder).build().use { indexBrowser ->
                    logger.info("list folder = {}", indexBrowser.getFolder())
                    for (fileInfo in indexBrowser.listFiles()) {
                        logger.info("\t\t{} {} {}", fileInfo.getType().name.substring(0, 1), fileInfo.getPath(), fileInfo.describeSize())
                    }
                }
            }
        }
        if (cmd.hasOption("I")) {
            updateIndex(syncthingClient)
            val folderInfo = StringBuilder()
            for (folder in syncthingClient.indexHandler.folderList) {
                folderInfo.append("\n\t\tfolder info : ")
                        .append(syncthingClient.indexHandler.getFolderInfo(folder))
                folderInfo.append("\n\t\tfolder stats : ")
                        .append(syncthingClient.indexHandler.newFolderBrowser().getFolderStats(folder).dumpInfo())
                        .append("\n")
            }
            logger.info("folders:\n{}\n", folderInfo.toString())
        }
        if (cmd.hasOption("li")) {
            var folderInfo = ""
            for (folder in syncthingClient.indexHandler.folderList) {
                folderInfo += "\n\t\tfolder info : " + syncthingClient.indexHandler.getFolderInfo(folder)
                folderInfo += "\n\t\tfolder stats : " + syncthingClient.indexHandler.newFolderBrowser().getFolderStats(folder).dumpInfo() + "\n"
            }
            logger.info("folders:\n{}\n", folderInfo)
        }
        if (cmd.hasOption("lp")) {
            syncthingClient.discoveryHandler.newDeviceAddressSupplier().use { deviceAddressSupplier ->
                var deviceAddressesStr = ""
                for (deviceAddress in Lists.newArrayList<DeviceAddress>(deviceAddressSupplier)) {
                    deviceAddressesStr += "\n\t\t" + deviceAddress.getDeviceId() + " : " + deviceAddress.getAddress()
                }
                logger.info("device addresses:\n{}\n", deviceAddressesStr)
            }
        }
        if (cmd.hasOption("s")) {
            val term = cmd.getOptionValue("s")
            syncthingClient.indexHandler.newIndexFinderBuilder().build().use { indexFinder ->
                updateIndex(syncthingClient)
                logger.info("search term = '{}'", term)
                val event = indexFinder.doSearch(term)
                if (event.hasGoodResults()) {
                    logger.info("search results for term = '{}' :", term)
                    for (fileInfo in event.getResultList()) {
                        logger.info("\t\t{} {} {}", fileInfo.getType().name.substring(0, 1), fileInfo.getPath(), fileInfo.describeSize())
                    }
                } else if (event.hasTooManyResults()) {
                    logger.info("too many results found for term = '{}'", term)
                } else {
                    logger.info("no result found for term = '{}'", term)
                }
            }
        }
        IOUtils.closeQuietly(configuration)
        syncthingClient.close()
    }

    @Throws(InterruptedException::class)
    private fun updateIndex(client: SyncthingClient) {
        val latch = CountDownLatch(1)
        client.updateIndexFromPeers { successes, failures ->
            logger.info("Index update successful: {}", successes)
            logger.info("Index update failed: {}", failures)
            latch.countDown()
        }
        latch.await()
    }

}
