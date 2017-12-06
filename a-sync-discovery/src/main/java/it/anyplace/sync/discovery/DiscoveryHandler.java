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
package it.anyplace.sync.discovery;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import it.anyplace.sync.core.beans.DeviceAddress;
import it.anyplace.sync.core.configuration.ConfigurationService;
import it.anyplace.sync.core.events.DeviceAddressReceivedEvent;
import it.anyplace.sync.core.interfaces.DeviceAddressRepository;
import it.anyplace.sync.core.utils.ExecutorUtils;
import it.anyplace.sync.discovery.protocol.GlobalDiscoveryHandler;
import it.anyplace.sync.discovery.protocol.LocalDiscoveryHandler;
import it.anyplace.sync.discovery.utils.AddressRanker;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author aleph
 */
public final class DiscoveryHandler implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ConfigurationService configuration;
    private final GlobalDiscoveryHandler globalDiscoveryHandler;
    private final LocalDiscoveryHandler localDiscoveryHandler;
    private final DeviceAddressRepository deviceAddressRepository;
    private final EventBus eventBus = new EventBus();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Map<Pair<String, String>, DeviceAddress> deviceAddressMap = Collections.synchronizedMap(Maps.<Pair<String, String>, DeviceAddress>newHashMap());
    private boolean isClosed = false;

    public DiscoveryHandler(ConfigurationService configuration, DeviceAddressRepository deviceAddressRepository) {
        logger.info("init");
        this.configuration = configuration;
        this.deviceAddressRepository = deviceAddressRepository;
        localDiscoveryHandler = new LocalDiscoveryHandler(configuration);
        globalDiscoveryHandler = new GlobalDiscoveryHandler(configuration);
        localDiscoveryHandler.getEventBus().register(new Object() {
            @Subscribe
            public void handleMessageReceivedEvent(LocalDiscoveryHandler.MessageReceivedEvent event) {
                logger.info("received device address list from local discovery");
                processDeviceAddressBg(event.getDeviceAddresses());
            }
        });
    }

    private boolean shouldLoadFromDb = true;
    private boolean shouldLoadFromGlobal = true;
    private boolean shouldStartLocalDiscovery = true;

    private void updateAddressesBg() {
        if (shouldLoadFromDb) {
            shouldLoadFromDb = false;
            executorService.submit(() -> {
                List<DeviceAddress> list = DiscoveryHandler.this.deviceAddressRepository.findAllDeviceAddress();
                logger.info("received device address list from database");
                processDeviceAddressBg(list);
            });
        }
        if (shouldStartLocalDiscovery) {
            shouldStartLocalDiscovery = false;
            localDiscoveryHandler.startListener();
            localDiscoveryHandler.sendAnnounceMessage();
        }
        if (shouldLoadFromGlobal) {
            shouldLoadFromGlobal = false; //TODO timeout for reload
            executorService.submit(() -> {
                for (String deviceId : DiscoveryHandler.this.configuration.getPeerIds()) {
                    List<DeviceAddress> list = globalDiscoveryHandler.query(deviceId);
                    logger.info("received device address list from global discovery");
                    processDeviceAddressBg(list);
                }
            });
        }
    }

    private void processDeviceAddressBg(final Iterable<DeviceAddress> deviceAddresses) {
        if (isClosed) {
            logger.debug("discarding device addresses, discovery handler already closed");
        } else {
            executorService.submit(() -> {
                logger.info("processing device address list");
                List<DeviceAddress> list = Lists.newArrayList(deviceAddresses);
                final Set<String> peers = Sets.newHashSet(configuration.getPeerIds());
                //do not process address already processed
                Iterables.removeIf(list, deviceAddress ->
                        !peers.contains(deviceAddress.getDeviceId()) || deviceAddressMap.containsKey(Pair.of(deviceAddress.getDeviceId(), deviceAddress.getAddress())));
                list = AddressRanker.testAndRank(list);
                for (DeviceAddress deviceAddress : list) {
                    putDeviceAddress(deviceAddress);
                }
            });
        }
    }

    private void putDeviceAddress(final DeviceAddress deviceAddress) {
        logger.info("acquired device address = {}", deviceAddress);
        deviceAddressMap.put(Pair.of(deviceAddress.getDeviceId(), deviceAddress.getAddress()), deviceAddress);
        deviceAddressRepository.updateDeviceAddress(deviceAddress);
        eventBus.post(new DeviceAddressUpdateEvent() {
            @Override
            public DeviceAddress getDeviceAddress() {
                return deviceAddress;
            }
        });
    }

    public EventBus getEventBus() {
        return eventBus;
    }

    public DeviceAddressSupplier newDeviceAddressSupplier() {
        DeviceAddressSupplier deviceAddressSupplier = new DeviceAddressSupplier(this);
        updateAddressesBg();
        return deviceAddressSupplier;
    }

    public List<DeviceAddress> getAllWorkingDeviceAddresses() {
        return Lists.newArrayList(Iterables.filter(deviceAddressMap.values(), DeviceAddress::isWorking));
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            if (localDiscoveryHandler != null) {
                localDiscoveryHandler.close();
            }
            if (globalDiscoveryHandler != null) {
                globalDiscoveryHandler.close();
            }
            executorService.shutdown();
            ExecutorUtils.awaitTerminationSafe(executorService);
        }
    }

    public abstract class DeviceAddressUpdateEvent implements DeviceAddressReceivedEvent {

        public abstract DeviceAddress getDeviceAddress();

        @Override
        public List<DeviceAddress> getDeviceAddresses() {
            return Collections.singletonList(getDeviceAddress());
        }
    }

}
