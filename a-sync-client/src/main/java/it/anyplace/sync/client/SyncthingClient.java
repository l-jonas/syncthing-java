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
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import it.anyplace.sync.bep.BlockExchangeConnectionHandler;
import it.anyplace.sync.bep.BlockPuller;
import it.anyplace.sync.bep.BlockPuller.FileDownloadObserver;
import it.anyplace.sync.bep.BlockPusher;
import it.anyplace.sync.bep.BlockPusher.FileUploadObserver;
import it.anyplace.sync.bep.IndexHandler;
import it.anyplace.sync.core.beans.DeviceAddress;
import it.anyplace.sync.core.beans.FileBlocks;
import it.anyplace.sync.core.beans.FileInfo;
import it.anyplace.sync.core.cache.BlockCache;
import it.anyplace.sync.core.configuration.ConfigurationService;
import it.anyplace.sync.core.security.KeystoreHandler;
import it.anyplace.sync.devices.DevicesHandler;
import it.anyplace.sync.discovery.DeviceAddressSupplier;
import it.anyplace.sync.discovery.DiscoveryHandler;
import it.anyplace.sync.repository.repo.SqlRepository;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStoreException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 * @author aleph
 */
public final class SyncthingClient implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ConfigurationService configuration;
    private final DiscoveryHandler discoveryHandler;
    private final SqlRepository sqlRepository;
    private final IndexHandler indexHandler;
    private final List<BlockExchangeConnectionHandler> connections = Collections.synchronizedList(Lists.<BlockExchangeConnectionHandler>newArrayList());
    private final DevicesHandler devicesHandler;

    public SyncthingClient(ConfigurationService configuration) {
        this.configuration = configuration;
        this.sqlRepository = new SqlRepository(configuration);
        indexHandler = new IndexHandler(configuration, sqlRepository, sqlRepository);
        discoveryHandler = new DiscoveryHandler(configuration, sqlRepository);
        devicesHandler = new DevicesHandler(configuration);
        discoveryHandler.getEventBus().register(devicesHandler);
    }

    public void clearCacheAndIndex() {
        logger.info("clear cache");
        indexHandler.clearIndex();
        configuration.edit().setFolders(Collections.emptyList()).persistLater();
        BlockCache.getBlockCache(configuration).clear();
    }

    public DevicesHandler getDevicesHandler() {
        return devicesHandler;
    }

    private BlockExchangeConnectionHandler openConnection(DeviceAddress deviceAddress) throws IOException, KeystoreHandler.CryptoException {
        final BlockExchangeConnectionHandler connectionHandler = new BlockExchangeConnectionHandler(configuration, deviceAddress);
        connectionHandler.setIndexHandler(indexHandler);
        connectionHandler.getEventBus().register(indexHandler);
        connectionHandler.getEventBus().register(devicesHandler);
        final AtomicBoolean shouldRestartForNewFolder = new AtomicBoolean(false);
        connectionHandler.getEventBus().register(new Object() {
            @Subscribe
            public void handleConnectionClosedEvent(BlockExchangeConnectionHandler.ConnectionClosedEvent event) {
                connections.remove(connectionHandler);
            }

            @Subscribe
            public void handleNewFolderSharedEvent(BlockExchangeConnectionHandler.NewFolderSharedEvent event) {
                shouldRestartForNewFolder.set(true);
            }
        });
        connectionHandler.connect();
        connections.add(connectionHandler);
        if (shouldRestartForNewFolder.get()) {
            logger.info("restart connection for new folder shared");
            connectionHandler.close();
            return openConnection(deviceAddress);
        } else {
            return connectionHandler;
        }
    }

    public interface OnConnectionAcquiredListener {
        void onConnectionAcquired(BlockExchangeConnectionHandler connection);
    }

    private interface OnConnectingCompleteListener {
        void onConnectingComplete();
    }

    public interface OnIndexUpdateCompleteListener {
        void onIndexUpdateComplete(Set<String> successes, Set<String> failures);
    }

    public interface OnErrorListener {
        // TODO: should pass some kind of error info
        void onError();
    }

    public interface OnFileUploadObserverReadyListener {
        void onFileUploadObserverReady(FileUploadObserver fileUploadObserver);
    }

    public interface OnFileDownloadObserverReadyListener {
        void onFileDownloadObserverReady(FileDownloadObserver fileDownloadObserver);
    }

    public interface OnIndexEditObserverReadyListener {
        void onIndexEditObserverReady(BlockPusher.IndexEditObserver indexEditObserver);
    }

    private BlockExchangeConnectionHandler getDeviceConnection(DeviceAddress address) throws IOException, KeystoreHandler.CryptoException {
        for (BlockExchangeConnectionHandler c : connections) {
            if (c.getAddress().equals(address)) {
                return c;
            }
        }
        return openConnection(address);
    }

    private void getConnectionForFolder(String folder, OnConnectionAcquiredListener listener, OnErrorListener errorListener) {
        AtomicBoolean isConnected = new AtomicBoolean(false);
        getPeerConnections(connection -> {
            if (connection.hasFolder(folder)) {
                listener.onConnectionAcquired(connection);
                isConnected.set(true);
            } else {
                connection.close();
            }
        }, () -> {
            if (!isConnected.get()) {
                errorListener.onError();
            }
        });
    }

    // TODO: should get rid of this and just have getDeviceConnection() connect to single device
    private void getPeerConnections(OnConnectionAcquiredListener listener, OnConnectingCompleteListener completeListener) {
        new Thread(() -> {
            DeviceAddressSupplier addressesSupplier = discoveryHandler.newDeviceAddressSupplier();
            Set<String> connectedDevices = Sets.newHashSet();
            for (final DeviceAddress deviceAddress : addressesSupplier) {
                if (connectedDevices.contains(deviceAddress.getDeviceId())) {
                    continue;
                }
                try {
                    BlockExchangeConnectionHandler connection = getDeviceConnection(deviceAddress);
                    connectedDevices.add(deviceAddress.getDeviceId());
                    listener.onConnectionAcquired(connection);
                } catch (IOException | KeystoreHandler.CryptoException e) {
                    logger.warn("error connecting to device = {}", deviceAddress, e);
                }
            }
            completeListener.onConnectingComplete();
            addressesSupplier.close();
        }).start();
    }

    public void updateIndexFromPeers(OnIndexUpdateCompleteListener listener) {
        // TODO: if there is already an index update in progress, do nothing
        //       this should probably be handled in IndexHandler
        //       at the moment, this is handled on the Android side
        Set<String> indexUpdateComplete = Sets.newHashSet();
        getPeerConnections(connection -> {
            try {
                indexHandler.waitForRemoteIndexAquired(connection);
                indexUpdateComplete.add(connection.getDeviceId());
            } catch (InterruptedException ex) {
                logger.warn("exception while waiting for index", ex);
            }
        }, () -> {
            Set<String> indexUpdateFailed = Sets.difference(configuration.getPeerIds(), indexUpdateComplete);
            listener.onIndexUpdateComplete(indexUpdateComplete, indexUpdateFailed);
        });
    }

    public void pullFile(String folder, String path, OnFileDownloadObserverReadyListener listener,
                                         OnErrorListener errorListener) {
        getConnectionForFolder(folder, connection -> {
            try {
                Pair<FileInfo, FileBlocks> fileInfoAndBlocks = indexHandler.waitForRemoteIndexAquired(connection).getFileInfoAndBlocksByPath(folder, path);
                checkNotNull(fileInfoAndBlocks, "file not found in local index for folder = %s path = %s", folder, path);
                FileDownloadObserver observer = new BlockPuller(configuration, connection, false).pullBlocks(fileInfoAndBlocks.getValue());
                listener.onFileDownloadObserverReady(observer);
            } catch (InterruptedException e) {
                logger.warn("Failed to pull file", e);
                errorListener.onError();
            }
        }, errorListener);
    }

    public void pushFile(InputStream data, String folder, String path, OnFileUploadObserverReadyListener listener,
                                       OnErrorListener errorListener) {
        getConnectionForFolder(folder, connection -> {
            try {
                BlockPusher pusher = new BlockPusher(configuration, connection, false);
                pusher.withIndexHandler(indexHandler);
                FileInfo fileInfo = indexHandler.waitForRemoteIndexAquired(connection).getFileInfoByPath(folder, path);
                FileUploadObserver observer = pusher.pushFile(data, fileInfo, folder, path);
                listener.onFileUploadObserverReady(observer);
            } catch (InterruptedException e) {
                logger.warn("Failed to pull file", e);
                errorListener.onError();
            }
        }, errorListener);
    }

    public void pushDir(String folder, String path, OnIndexEditObserverReadyListener listener,
                                                 OnErrorListener errorListener) {
        getConnectionForFolder(folder, connection -> {
            BlockPusher pusher = new BlockPusher(configuration, connection, false);
            pusher.withIndexHandler(indexHandler);
            BlockPusher.IndexEditObserver observer = pusher.pushDir(folder, path);
            listener.onIndexEditObserverReady(observer);
        }, errorListener);
    }

    public void pushDelete(String folder, String path, OnIndexEditObserverReadyListener listener,
                                                    OnErrorListener errorListener) {
        getConnectionForFolder(folder, connection -> {
            try {
                BlockPusher pusher = new BlockPusher(configuration, connection, false);
                pusher.withIndexHandler(indexHandler);
                FileInfo fileInfo = indexHandler.waitForRemoteIndexAquired(connection).getFileInfoByPath(folder, path);
                BlockPusher.IndexEditObserver observer = pusher.pushDelete(fileInfo, folder, path);
                listener.onIndexEditObserverReady(observer);
            } catch (InterruptedException e) {
                logger.warn("Failed to push delete", e);
                errorListener.onError();
            }
        }, errorListener);
    }

    public DiscoveryHandler getDiscoveryHandler() {
        return discoveryHandler;
    }

    public IndexHandler getIndexHandler() {
        return indexHandler;
    }

    @Override
    public void close() {
        devicesHandler.close();
        discoveryHandler.close();
        for (BlockExchangeConnectionHandler connectionHandler : connections) {
            connectionHandler.close();
        }
        indexHandler.close();
        sqlRepository.close();
    }

}
