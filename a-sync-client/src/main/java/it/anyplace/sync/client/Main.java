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
package it.anyplace.sync.client;

import com.google.common.collect.Lists;
import it.anyplace.sync.bep.IndexBrowser;
import it.anyplace.sync.bep.IndexFinder;
import it.anyplace.sync.core.beans.DeviceAddress;
import it.anyplace.sync.core.beans.DeviceInfo;
import it.anyplace.sync.core.beans.FileInfo;
import it.anyplace.sync.core.configuration.ConfigurationService;
import it.anyplace.sync.core.security.KeystoreHandler;
import it.anyplace.sync.discovery.DeviceAddressSupplier;
import it.anyplace.sync.discovery.protocol.GlobalDiscoveryHandler;
import it.anyplace.sync.discovery.protocol.LocalDiscoveryHandler;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkArgument;

/**
 *
 * @author aleph
 */
public final class Main {

    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("C", "set-config", true, "set config file for s-client");
        options.addOption("c", "config", false, "dump config");
        options.addOption("sp", "set-peers", true, "set peer, or comma-separated list of peers");
        options.addOption("q", "query", true, "query directory server for device id");
        options.addOption("d", "discovery", true, "discovery local network for device id");
        options.addOption("p", "pull", true, "pull file from network");
        options.addOption("P", "push", true, "push file to network");
        options.addOption("o", "output", true, "set output file/directory");
        options.addOption("i", "input", true, "set input file/directory");
        options.addOption("lp", "list-peers", false, "list peer addresses");
        options.addOption("a", "address", true, "use this peer addresses");
        options.addOption("L", "list-remote", false, "list folder (root) content from network");
        options.addOption("I", "list-info", false, "dump folder info from network");
        options.addOption("li", "list-info", false, "list folder info from local db");
        options.addOption("s", "search", true, "search local index for <term>");
        options.addOption("D", "delete", true, "push delete to network");
        options.addOption("M", "mkdir", true, "push directory create to network");
        options.addOption("h", "help", false, "print help");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("s-client", options);
            return;
        }

        File configFile = cmd.hasOption("C") ? new File(cmd.getOptionValue("C")) : new File(System.getProperty("user.home"), ".s-client.properties");
        logger.info("using config file = {}", configFile);
        ConfigurationService configuration = ConfigurationService.newLoader().loadFrom(configFile);
        FileUtils.cleanDirectory(configuration.getTemp());
        KeystoreHandler.newLoader().loadAndStore(configuration);
        SyncthingClient syncthingClient = new SyncthingClient(configuration);
        if (cmd.hasOption("c")) {
            logger.info("configuration =\n{}", configuration.newWriter().dumpToString());
        } else {
            logger.trace("configuration =\n{}", configuration.newWriter().dumpToString());
        }
        logger.debug("{}", configuration.getStorageInfo().dumpAvailableSpace());

        if (cmd.hasOption("sp")) {
            List<String> peers = Lists.newArrayList(Lists.transform(Arrays.asList(cmd.getOptionValue("sp").split(",")),
                    String::trim));
            logger.info("set peers = {}", peers);
            configuration.edit().setPeers(Collections.emptyList());
            for (String peer : peers) {
                KeystoreHandler.validateDeviceId(peer);
                configuration.edit().addPeers(new DeviceInfo(peer, null));
            }
            configuration.edit().persistNow();
        }

        if (cmd.hasOption("q")) {
            String deviceId = cmd.getOptionValue("q");
            logger.info("query device id = {}", deviceId);
            List<DeviceAddress> deviceAddresses = new GlobalDiscoveryHandler(configuration).query(deviceId);
            logger.info("server response = {}", deviceAddresses);
        }
        if (cmd.hasOption("d")) {
            String deviceId = cmd.getOptionValue("d");
            logger.info("discovery device id = {}", deviceId);
            Collection<DeviceAddress> deviceAddresses = new LocalDiscoveryHandler(configuration).queryAndClose(deviceId);
            logger.info("local response = {}", deviceAddresses);
        }

        if (cmd.hasOption("p")) {
            String folderAndPath = cmd.getOptionValue("p");
            logger.info("file path = {}", folderAndPath);
            String folder = folderAndPath.split(":")[0];
            String path = folderAndPath.split(":")[1];
            CountDownLatch latch = new CountDownLatch(1);
            syncthingClient.pullFile(folder, path, observer -> {
                try {
                    InputStream inputStream = observer.waitForComplete().getInputStream();
                    String fileName = syncthingClient.getIndexHandler().getFileInfoByPath(folder, path).getFileName();
                    File file;
                    if (cmd.hasOption("o")) {
                        File param = new File(cmd.getOptionValue("o"));
                        file = param.isDirectory() ? new File(param, fileName) : param;
                    } else {
                        file = new File(fileName);
                    }
                    FileUtils.copyInputStreamToFile(inputStream, file);
                    logger.info("saved file to = {}", file.getAbsolutePath());
                } catch (InterruptedException | IOException e) {
                    logger.warn("", e);
                }
            }, () -> logger.warn("Failed to pull file"));
            latch.await();
        }
        if (cmd.hasOption("P")) {
            String path = cmd.getOptionValue("P");
            File file = new File(cmd.getOptionValue("i"));
            checkArgument(!path.startsWith("/")); //TODO check path syntax
            logger.info("file path = {}", path);
            String folder = path.split(":")[0];
            path = path.split(":")[1];
            CountDownLatch latch = new CountDownLatch(1);
            syncthingClient.pushFile(new FileInputStream(file), folder, path, fileUploadObserver -> {
                while (!fileUploadObserver.isCompleted()) {
                    try {
                        fileUploadObserver.waitForProgressUpdate();
                    } catch (InterruptedException e) {
                        logger.warn("", e);
                    }
                    logger.debug("upload progress {}", fileUploadObserver.getProgressMessage());
                }
                latch.countDown();
            }, () -> logger.warn("Failed to upload file"));
            latch.await();
            logger.info("uploaded file to network");
        }
        if (cmd.hasOption("D")) {
            String path = cmd.getOptionValue("D");
            String folder = path.split(":")[0];
            path = path.split(":")[1];
            logger.info("delete path = {}", path);
            CountDownLatch latch = new CountDownLatch(1);
            syncthingClient.pushDelete(folder, path, observer -> {
                try {
                    observer.waitForComplete();
                } catch (InterruptedException e) {
                    logger.warn("", e);
                }
                latch.countDown();
            }, () -> logger.warn("Failed to delete path"));
            latch.await();
            logger.info("deleted path");
        }
        if (cmd.hasOption("M")) {
            String path = cmd.getOptionValue("M");
            String folder = path.split(":")[0];
            path = path.split(":")[1];
            logger.info("dir path = {}", path);
            CountDownLatch latch = new CountDownLatch(1);
            syncthingClient.pushDir(folder, path, observer -> {
                try {
                    observer.waitForComplete();
                } catch (InterruptedException e) {
                    logger.warn("", e);
                }
                latch.countDown();
            }, () -> logger.warn("Failed to push directory"));
            latch.await();
            logger.info("uploaded dir to network");
        }
        if (cmd.hasOption("L")) {
            updateIndex(syncthingClient);
            for (String folder : syncthingClient.getIndexHandler().getFolderList()) {
                try (IndexBrowser indexBrowser = syncthingClient.getIndexHandler().newIndexBrowserBuilder().setFolder(folder).build()) {
                    logger.info("list folder = {}", indexBrowser.getFolder());
                    for (FileInfo fileInfo : indexBrowser.listFiles()) {
                        logger.info("\t\t{} {} {}", fileInfo.getType().name().substring(0, 1), fileInfo.getPath(), fileInfo.describeSize());
                    }
                }
            }
        }
        if (cmd.hasOption("I")) {
            if (cmd.hasOption("a")) {
                String deviceId = cmd.getOptionValue("a").substring(0, 63), address = cmd.getOptionValue("a").substring(64);
                updateIndex(syncthingClient);
            } else {
                updateIndex(syncthingClient);
            }
            StringBuilder folderInfo = new StringBuilder();
            for (String folder : syncthingClient.getIndexHandler().getFolderList()) {
                folderInfo.append("\n\t\tfolder info : ")
                        .append(syncthingClient.getIndexHandler().getFolderInfo(folder));
                folderInfo.append("\n\t\tfolder stats : ")
                        .append(syncthingClient.getIndexHandler().newFolderBrowser().getFolderStats(folder).dumpInfo())
                        .append("\n");
            }
            logger.info("folders:\n{}\n", folderInfo.toString());
        }
        if (cmd.hasOption("li")) {
            String folderInfo = "";
            for (String folder : syncthingClient.getIndexHandler().getFolderList()) {
                folderInfo += "\n\t\tfolder info : " + syncthingClient.getIndexHandler().getFolderInfo(folder);
                folderInfo += "\n\t\tfolder stats : " + syncthingClient.getIndexHandler().newFolderBrowser().getFolderStats(folder).dumpInfo() + "\n";
            }
            logger.info("folders:\n{}\n", folderInfo);
        }
        if (cmd.hasOption("lp")) {
            try (DeviceAddressSupplier deviceAddressSupplier = syncthingClient.getDiscoveryHandler().newDeviceAddressSupplier()) {
                String deviceAddressesStr = "";
                for (DeviceAddress deviceAddress : Lists.newArrayList(deviceAddressSupplier)) {
                    deviceAddressesStr += "\n\t\t" + deviceAddress.getDeviceId() + " : " + deviceAddress.getAddress();
                }
                logger.info("device addresses:\n{}\n", deviceAddressesStr);
            }
        }
        if (cmd.hasOption("s")) {
            String term = cmd.getOptionValue("s");
            try (IndexFinder indexFinder = syncthingClient.getIndexHandler().newIndexFinderBuilder().build()) {
                updateIndex(syncthingClient);
                logger.info("search term = '{}'", term);
                IndexFinder.SearchCompletedEvent event = indexFinder.doSearch(term);
                if (event.hasGoodResults()) {
                    logger.info("search results for term = '{}' :", term);
                    for (FileInfo fileInfo : event.getResultList()) {
                        logger.info("\t\t{} {} {}", fileInfo.getType().name().substring(0, 1), fileInfo.getPath(), fileInfo.describeSize());
                    }
                } else if (event.hasTooManyResults()) {
                    logger.info("too many results found for term = '{}'", term);
                } else {
                    logger.info("no result found for term = '{}'", term);
                }
            }
        }
        IOUtils.closeQuietly(configuration);
        syncthingClient.close();
    }

    private static void updateIndex(SyncthingClient client) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        client.updateIndexFromPeers((successes, failures) -> {
            logger.info("Index update successful: {}", successes);
            logger.info("Index update failed: {}", failures);
            latch.countDown();
        });
        latch.await();
    }

}
