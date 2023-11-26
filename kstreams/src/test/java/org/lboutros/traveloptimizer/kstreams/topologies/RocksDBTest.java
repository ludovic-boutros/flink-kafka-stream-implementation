package org.lboutros.traveloptimizer.kstreams.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lboutros.traveloptimizer.kstreams.configuration.Constants;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.generator.DataGenerator;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;
import static org.lboutros.traveloptimizer.kstreams.configuration.Constants.StateStores.EARLY_REQUESTS_STATE_STORE;
import static org.lboutros.traveloptimizer.kstreams.configuration.Utils.readConfiguration;

@Slf4j
class RocksDBTest {
    private final Random R = new Random();

    private TopologyTestDriver testDriver;

    private Topology getTestTopology() {

        var builder = new StreamsBuilder();

        final StoreBuilder<KeyValueStore<String, String>> store =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(Constants.StateStores.A_STORE_NAME),
                        Serdes.String(),
                        Serdes.String()
                );

        builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()));
        builder.addStateStore(store);

        return builder.build();
    }

    @BeforeEach
    public void setup() throws ConfigurationException {
        Properties streamsConfiguration = readConfiguration("test.properties");

        // Init test driver
        testDriver = new TopologyTestDriver(getTestTopology(), streamsConfiguration, Instant.EPOCH);
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void cleanup() {
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSSTore() throws NoSuchFieldException, IllegalAccessException {
        Random R = new Random();

        MeteredKeyValueStore<String, CustomerTravelRequest> customerStateStore = (MeteredKeyValueStore) testDriver.getKeyValueStore(EARLY_REQUESTS_STATE_STORE);

        Field producerPrivateField = TopologyTestDriver.class.getDeclaredField("producer");
        producerPrivateField.setAccessible(true);
        MockProducer producer = (MockProducer) producerPrivateField.get(testDriver);

        // Change log topic's name travel-optimizer-earlyRequestStateStore-changelog

        // Insert 2M entries in the state
        for (int i = 0; i < 2_000_000; i++) {
            CustomerTravelRequest request = DataGenerator.generateCustomerTravelRequestData();
            request.setHugeDummyData(generateDummyData(R, 20));

            int nextInt = R.nextInt(1_000);
            request.setDepartureLocation(Integer.toString(R.nextInt(10_000_000) + 10_000_000));
            request.setArrivalLocation(Integer.toString(R.nextInt(50_000_000) + 10_000_000));


            customerStateStore.put(request.getDepartureLocation() + "#" + request.getArrivalLocation(), request);
            if (nextInt < 10) {
                // Set 1% random lines as delete prefixed ones
                customerStateStore.put("DELETE#" + request.getDepartureLocation() + "#" + request.getArrivalLocation(), request);
            } else {
                customerStateStore.put(request.getDepartureLocation() + "#" + request.getArrivalLocation(), request);
            }


            if (i % 100_000 == 0) {

                // Remove the internally stored fake produced record due to Test Driver mock producer to save heap
                producer.clear();

                // When
                long start = nanoTime();
                List<String> keys = new ArrayList<>();
                try (KeyValueIterator<String, CustomerTravelRequest> iterator = customerStateStore.prefixScan("DELETE#", Serdes.String().serializer())) {
                    var prefixNow = nanoTime();
                    long prefixScanTime = prefixNow - start;
                    while (iterator.hasNext() && keys.size() < 300) {
                        keys.add(iterator.next().key);
                    }
                    if (iterator.hasNext()) {
                        log.info("{}: {} entries: prefix scan took {} ns: first scanned key {}", i, customerStateStore.approximateNumEntries(), prefixScanTime, iterator.next().key);
                    }
                }

                if (!keys.isEmpty()) {

                    long startDelete = nanoTime();
                    log.info("Deleting {} keys...", keys.size());
                    keys.forEach(customerStateStore::delete);
                    long afterDelete = nanoTime();

                    log.info("Deleting done in {} ns", afterDelete - startDelete);

                }
            }
        }


        while (customerStateStore.approximateNumEntries() > 1000) {
            int nbEntryDeleted = 0;
            int nbEntryAdded = 0;
            int nbDeleteEntryAdded = 0;

            long startLoop = currentTimeMillis();

            try (var it = customerStateStore.all()) {


                while (it.hasNext()) {

                    var next = it.next();
                    var randomNumber = R.nextInt(100);
                    if (randomNumber == 42) {
                        customerStateStore.delete(next.key);
                        nbEntryDeleted++;
                    }

                    if (randomNumber == 0) {
                        CustomerTravelRequest request = DataGenerator.generateCustomerTravelRequestData();
                        request.setHugeDummyData(generateDummyData(R, 10));

                        int nextInt = R.nextInt(1_000);
                        request.setDepartureLocation(Integer.toString(R.nextInt(10_000_000) + 10_000_000));
                        request.setArrivalLocation(Integer.toString(R.nextInt(50_000_000) + 10_000_000));
                        if (nextInt < 10) {
                            customerStateStore.put("DELETE#" + request.getDepartureLocation() + "#" + request.getArrivalLocation(), request);
                            nbDeleteEntryAdded++;
                        } else {
                            customerStateStore.put(request.getDepartureLocation() + "#" + request.getArrivalLocation(), request);
                            nbEntryAdded++;
                        }
                    }
                }
                log.info("deleted " + nbEntryDeleted + " entries, added " + nbEntryAdded + ", marked for deletion " + nbDeleteEntryAdded);
                long loopTime = currentTimeMillis() - startLoop;

                log.info("Full scan loop took " + loopTime + " ms");

                long start = System.nanoTime();
                try (KeyValueIterator<String, CustomerTravelRequest> iterator = customerStateStore.prefixScan("DELETE#", Serdes.String().serializer())) {
                    long prefixScanTime = System.nanoTime() - start;

                    if (iterator.hasNext()) {
                        log.info("{} entries in store -- prefix scan took {} ns -- first key : {}", customerStateStore.approximateNumEntries(), prefixScanTime, iterator.next().key);
                    }
                }
            }
        }


    }

    private String generateDummyData(Random R, int count) {
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < count; i++) {
            builder.append(R.nextInt(256));
        }
        return builder.toString();
    }
}