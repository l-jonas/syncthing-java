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

class Main(private val commandLine: CommandLine) {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val options = generateOptions()
            val parser = DefaultParser()
            val cmd = parser.parse(options, args)
            if (cmd.hasOption("h")) {
                val formatter = HelpFormatter()
                formatter.printHelp("s-client", options)
                return
            }
            val configFile = if (cmd.hasOption("C")) File(cmd.getOptionValue("C"))
            else                                     File(System.getProperty("user.home"), ".s-client.properties")

            ConfigurationService.newLoader().loadFrom(configFile).use { configuration ->
                SyncthingClient(configuration).use { syncthingClient ->
                    System.out.println("using config file = $configFile")
                    FileUtils.cleanDirectory(configuration.temp)
                    KeystoreHandler.newLoader().loadAndStore(configuration)
                    System.out.println("configuration =\n${configuration.newWriter().dumpToString()}")
                    System.out.println(configuration.storageInfo.dumpAvailableSpace())
                    val main = Main(cmd)
                    cmd.options.forEach { main.handleOption(it, configuration, syncthingClient) }
                }
            }
        }

        private fun generateOptions(): Options {
            val options = Options()
            options.addOption("C", "set-config", true, "set config file for s-client")
            options.addOption("c", "config", false, "dump config")
            options.addOption("S", "set-peers", true, "set peer, or comma-separated list of peers")
            options.addOption("q", "query", true, "query directory server for device id")
            options.addOption("d", "discovery", true, "discovery local network for device id")
            options.addOption("p", "pull", true, "pull file from network")
            options.addOption("P", "push", true, "push file to network")
            options.addOption("o", "output", true, "set output file/directory")
            options.addOption("i", "input", true, "set input file/directory")
            options.addOption("a", "list-peers", false, "list peer addresses")
            options.addOption("a", "address", true, "use this peer addresses")
            options.addOption("L", "list-remote", false, "list folder (root) content from network")
            options.addOption("I", "list-info", false, "dump folder info from network")
            options.addOption("l", "list-info", false, "list folder info from local db")
            options.addOption("s", "search", true, "search local index for <term>")
            options.addOption("D", "delete", true, "push delete to network")
            options.addOption("M", "mkdir", true, "push directory create to network")
            options.addOption("h", "help", false, "print help")
            return options
        }
    }

    private val logger = LoggerFactory.getLogger(Main::class.java)

    private fun handleOption(option: Option, configuration: ConfigurationService, syncthingClient: SyncthingClient) {
        when (option.opt) {
            "S" -> {
                val peers = option.value
                        .split(",")
                        .map { it.trim() }
                        .filterNot { it.isEmpty() }
                        .toList()
                System.out.println("set peers = $peers")
                configuration.edit().setPeers(emptyList<DeviceInfo>())
                for (peer in peers) {
                    KeystoreHandler.validateDeviceId(peer)
                    configuration.edit().addPeers(DeviceInfo(peer, null))
                }
                configuration.edit().persistNow()
            }
            "q" -> {
                val deviceId = option.value
                System.out.println("query device id = $deviceId")
                val deviceAddresses = GlobalDiscoveryHandler(configuration).query(deviceId)
                System.out.println("server response = $deviceAddresses")
            }
            "d" -> {
                val deviceId = option.value
                System.out.println("discovery device id = $deviceId")
                val deviceAddresses = LocalDiscoveryHandler(configuration).queryAndClose(deviceId)
                System.out.println("local response = $deviceAddresses")
            }

            "p" -> {
                val folderAndPath = option.value
                System.out.println("file path = $folderAndPath")
                val folder = folderAndPath.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[0]
                val path = folderAndPath.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[1]
                val latch = CountDownLatch(1)
                syncthingClient.pullFile(folder, path, { observer ->
                    try {
                        val inputStream = observer.waitForComplete().inputStream
                        val fileName = syncthingClient.indexHandler.getFileInfoByPath(folder, path)!!.fileName
                        val file  =
                            if (commandLine.hasOption("o")) {
                                val param = File(commandLine.getOptionValue("o"))
                                if (param.isDirectory) File(param, fileName) else param
                            } else {
                                File(fileName)
                            }
                        FileUtils.copyInputStreamToFile(inputStream, file)
                        System.out.println("saved file to = $file.absolutePath")
                    } catch (e: InterruptedException) {
                        logger.warn("", e)
                    } catch (e: IOException) {
                        logger.warn("", e)
                    }
                }) { logger.warn("Failed to pull file") }
                latch.await()
            }
            "P" -> {
                var path = option.value
                val file = File(commandLine.getOptionValue("i"))
                checkArgument(!path.startsWith("/")) //TODO check path syntax
                System.out.println("file path = $path")
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

                        System.out.println("upload progress ${fileUploadObserver.progressMessage}")
                    }
                    latch.countDown()
                }) { logger.warn("Failed to upload file") }
                latch.await()
                System.out.println("uploaded file to network")
            }
            "D" -> {
                var path = option.value
                val folder = path.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[0]
                path = path.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[1]
                System.out.println("delete path = $path")
                val latch = CountDownLatch(1)
                syncthingClient.pushDelete(folder, path, { observer ->
                    try {
                        observer.waitForComplete()
                    } catch (e: InterruptedException) {
                        logger.warn("", e)
                    }

                    latch.countDown()
                }) { System.out.println("Failed to delete path") }
                latch.await()
                System.out.println("deleted path")
            }
            "M" -> {
                var path = option.value
                val folder = path.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[0]
                path = path.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()[1]
                System.out.println("dir path = $path")
                val latch = CountDownLatch(1)
                syncthingClient.pushDir(folder, path, { observer ->
                    try {
                        observer.waitForComplete()
                    } catch (e: InterruptedException) {
                        logger.warn("", e)
                    }

                    latch.countDown()
                }) { System.out.println("Failed to push directory") }
                latch.await()
                System.out.println("uploaded dir to network")
            }
            "L" -> {
                updateIndex(syncthingClient)
                for (folder in syncthingClient.indexHandler.folderList) {
                    syncthingClient.indexHandler.newIndexBrowserBuilder().setFolder(folder).build().use { indexBrowser ->
                        System.out.println("list folder = ${indexBrowser.folder}")
                        for (fileInfo in indexBrowser.listFiles()) {
                            System.out.println("${fileInfo.type.name.substring(0, 1)}\t${fileInfo.describeSize()}\t${fileInfo.path}")
                        }
                    }
                }
            }
            "I" -> {
                updateIndex(syncthingClient)
                val folderInfo = StringBuilder()
                for (folder in syncthingClient.indexHandler.folderList) {
                    folderInfo.append("\nfolder info: ")
                            .append(syncthingClient.indexHandler.getFolderInfo(folder))
                    folderInfo.append("\nfolder stats: ")
                            .append(syncthingClient.indexHandler.newFolderBrowser().getFolderStats(folder).dumpInfo())
                            .append("\n")
                }
                System.out.println("folders:\n$folderInfo\n")
            }
            "l" -> {
                var folderInfo = ""
                for (folder in syncthingClient.indexHandler.folderList) {
                    folderInfo += "\nfolder info: " + syncthingClient.indexHandler.getFolderInfo(folder)
                    folderInfo += "\nfolder stats: " + syncthingClient.indexHandler.newFolderBrowser().getFolderStats(folder).dumpInfo() + "\n"
                }
                System.out.println("folders:\n$folderInfo\n")
            }
            "a" -> {
                syncthingClient.discoveryHandler.newDeviceAddressSupplier().use { deviceAddressSupplier ->
                    var deviceAddressesStr = ""
                    for (deviceAddress in Lists.newArrayList<DeviceAddress>(deviceAddressSupplier)) {
                        deviceAddressesStr += "\n" + deviceAddress.deviceId + " : " + deviceAddress.address
                    }
                    System.out.println("device addresses:\n$deviceAddressesStr\n")
                }
            }
            "s" -> {
                val term = option.value
                syncthingClient.indexHandler.newIndexFinderBuilder().build().use { indexFinder ->
                    updateIndex(syncthingClient)
                    val event = indexFinder.doSearch(term)
                    when {
                        event.hasGoodResults() -> {
                            System.out.println("search results for '$term' :")
                            for (fileInfo in event.resultList) {
                                System.out.println("${fileInfo.type.name.substring(0, 1)}\t${fileInfo.describeSize()}\t${fileInfo.path}")
                            }
                        }
                        event.hasTooManyResults() -> System.out.println("too many results found for = '$term'")
                        else -> System.out.println("no result found for = '$term'")
                    }
                }
            }
        }
    }

    @Throws(InterruptedException::class)
    private fun updateIndex(client: SyncthingClient) {
        val latch = CountDownLatch(1)
        client.updateIndexFromPeers { _, failures ->
            if (failures.isEmpty()) {
                System.out.println("Index update successful for all peers")
            } else {
                System.out.println("Index update failed for devices: $failures")
            }
            latch.countDown()
        }
        latch.await()
    }

}
