package org.lboutros.traveloptimizer.kstreams.topologies;

import lombok.Builder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.lboutros.traveloptimizer.GlobalConstants;
import org.lboutros.traveloptimizer.kstreams.configuration.Constants;
import org.lboutros.traveloptimizer.kstreams.topologies.models.TimeTableEntry;
import org.lboutros.traveloptimizer.kstreams.topologies.processors.CustomerTravelRequestProcessor;
import org.lboutros.traveloptimizer.kstreams.topologies.processors.DepartedProcessor;
import org.lboutros.traveloptimizer.kstreams.topologies.processors.TimeTableUpdateProcessor;
import org.lboutros.traveloptimizer.model.*;

import java.util.Map;
import java.util.Properties;

import static org.lboutros.traveloptimizer.kstreams.configuration.Utils.toMap;

public class TravelOptimizerTopologySupplier extends TopologySupplierBase {
    @Builder
    public TravelOptimizerTopologySupplier(Properties streamsConfiguration) {
        super(streamsConfiguration, "travel-optimizer");
    }

    @Override
    public Topology get() {
        // Init
        Map<String, Object> serdeConfiguration = toMap(getStreamsConfiguration());

        Constants.Serdes.TRAVEL_ALERTS_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.PLANE_TIME_UPDATE_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.TRAIN_TIME_UPDATE_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.TIME_UPDATE_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE.configure(serdeConfiguration, false);

        // Create state stores
        final StoreBuilder<KeyValueStore<String, TimeTableEntry>> availableConnections =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE),
                        Serdes.String(),
                        Constants.Serdes.TIME_UPDATE_SERDE
                );

        final StoreBuilder<KeyValueStore<String, TravelAlert>> activeRequests =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE),
                        Serdes.String(),
                        Constants.Serdes.TRAVEL_ALERTS_SERDE
                );

        final StoreBuilder<KeyValueStore<String, CustomerTravelRequest>> earlyRequests =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(Constants.StateStores.EARLY_REQUESTS_STATE_STORE),
                        Serdes.String(),
                        Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE
                );

        getStreamsBuilder().addStateStore(availableConnections);
        getStreamsBuilder().addStateStore(activeRequests);
        getStreamsBuilder().addStateStore(earlyRequests);

        // Get the customer travel request stream
        final KStream<String, CustomerTravelRequest> customerTravelRequestStream = getStreamsBuilder().stream(
                GlobalConstants.Topics.CUSTOMER_TRAVEL_REQUEST_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE));

        // Get train time table stream
        final KStream<String, TrainTimeTableUpdate> trainTimeTableUpdateStream = getStreamsBuilder().stream(
                GlobalConstants.Topics.TRAIN_TIME_UPDATE_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.TRAIN_TIME_UPDATE_SERDE));

        // Get plane time table stream
        final KStream<String, PlaneTimeTableUpdate> planeTimeTableUpdateStream = getStreamsBuilder().stream(
                GlobalConstants.Topics.PLANE_TIME_UPDATE_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.PLANE_TIME_UPDATE_SERDE));

        // Get plane time table stream
        final KStream<String, Departure> departureStream = getStreamsBuilder().stream(
                GlobalConstants.Topics.DEPARTURE_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.DEPARTURE_SERDE));

        // Now select key and repartition all streams
        KStream<String, CustomerTravelRequest> rekeyedCustomerTravelRequestStream = customerTravelRequestStream
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE)
                        .withName(Constants.InternalTopics.REKEYED_CUSTOMER_TRAVEL_REQUEST_TOPIC));

        KStream<String, Departure> rekeyedDepartureStream = departureStream
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), Constants.Serdes.DEPARTURE_SERDE)
                        .withName(Constants.InternalTopics.REKEYED_DEPARTURE_TOPIC));

        // We map train and plane time table update to generic time table updates in order to use only one state store for both streams
        KStream<String, TimeTableEntry> rekeyedTrainTimeTableUpdateStream = trainTimeTableUpdateStream
                .mapValues(TimeTableEntry::fromTrainTimeTableUpdate)
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), Constants.Serdes.TIME_UPDATE_SERDE)
                        .withName(Constants.InternalTopics.REKEYED_TRAIN_TIME_UPDATE_TOPIC));

        KStream<String, TimeTableEntry> rekeyedPlaneTimeTableUpdateStream = planeTimeTableUpdateStream
                .mapValues(TimeTableEntry::fromPlaneTimeTableUpdate)
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), Constants.Serdes.TIME_UPDATE_SERDE)
                        .withName(Constants.InternalTopics.REKEYED_PLANE_TIME_UPDATE_TOPIC));

        // Process each stream and produce travel alerts
        KStream<String, TravelAlert> trainTimeTableUpdateTravelAlertStream = rekeyedTrainTimeTableUpdateStream
                .process(TimeTableUpdateProcessor::new,
                        Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE,
                        Constants.StateStores.EARLY_REQUESTS_STATE_STORE,
                        Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);

        KStream<String, TravelAlert> planeTimeTableUpdateTravelAlertStream = rekeyedPlaneTimeTableUpdateStream
                .process(TimeTableUpdateProcessor::new,
                        Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE,
                        Constants.StateStores.EARLY_REQUESTS_STATE_STORE,
                        Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);

        KStream<String, TravelAlert> customerTravelRequestTravelAlertStream = rekeyedCustomerTravelRequestStream
                .process(CustomerTravelRequestProcessor::new,
                        Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE,
                        Constants.StateStores.EARLY_REQUESTS_STATE_STORE,
                        Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);

        KStream<String, TravelAlert> departedTravelAlertStream = rekeyedDepartureStream
                .process(DepartedProcessor::new,
                        Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE,
                        Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);

        // Merge everything
        trainTimeTableUpdateTravelAlertStream.merge(planeTimeTableUpdateTravelAlertStream)
                .merge(customerTravelRequestTravelAlertStream)
                .merge(departedTravelAlertStream)
                .to(GlobalConstants.Topics.TRAVEL_ALERTS_TOPIC,
                        Produced.with(Serdes.String(), Constants.Serdes.TRAVEL_ALERTS_SERDE));

        return getStreamsBuilder().build();
    }
}
