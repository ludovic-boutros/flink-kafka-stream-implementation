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
import org.lboutros.traveloptimizer.kstreams.models.TimeTableUpdate;
import org.lboutros.traveloptimizer.kstreams.processors.CustomerTravelRequestProcessor;
import org.lboutros.traveloptimizer.kstreams.processors.TimeTableUpdateProcessor;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TravelAlert;

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
        final StoreBuilder<KeyValueStore<String, TimeTableUpdate>> availableConnections =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE),
                        Serdes.String(),
                        Constants.Serdes.TIME_UPDATE_SERDE
                );

        final StoreBuilder<KeyValueStore<String, CustomerTravelRequest>> activeRequests =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(Constants.StateStores.ACTIVE_REQUESTS_STATE_STORE),
                        Serdes.String(),
                        Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE
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

        // Get the customer travel request stream
        final KStream<String, TrainTimeTableUpdate> trainTimeTableUpdateStream = getStreamsBuilder().stream(
                GlobalConstants.Topics.TRAIN_TIME_UPDATE_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.TRAIN_TIME_UPDATE_SERDE));

        // Get the customer travel request stream
        final KStream<String, PlaneTimeTableUpdate> planeTimeTableUpdateStream = getStreamsBuilder().stream(
                GlobalConstants.Topics.PLANE_TIME_UPDATE_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.PLANE_TIME_UPDATE_SERDE));

        // Now select key and repartition all streams
        KStream<String, CustomerTravelRequest> rekeyedCustomerTravelRequestStream = customerTravelRequestStream
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE)
                        .withName(Constants.InternalTopics.REKEYED_CUSTOMER_TRAVEL_REQUEST_TOPIC));

        // We map train and plane time table update to generic time table updates in order to use only one state store for both streams
        KStream<String, TimeTableUpdate> rekeyedTrainTimeTableUpdateStream = trainTimeTableUpdateStream
                .mapValues(TimeTableUpdate::fromTrainTimeTableUpdate)
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), Constants.Serdes.TIME_UPDATE_SERDE)
                        .withName(Constants.InternalTopics.REKEYED_TRAIN_TIME_UPDATE_TOPIC));

        KStream<String, TimeTableUpdate> rekeyedPlaneTimeTableUpdateStream = planeTimeTableUpdateStream
                .mapValues(TimeTableUpdate::fromPlaneTimeTableUpdate)
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), Constants.Serdes.TIME_UPDATE_SERDE)
                        .withName(Constants.InternalTopics.REKEYED_PLANE_TIME_UPDATE_TOPIC));

        // Process each stream and produce travel alerts
        KStream<String, TravelAlert> trainTimeTableUpdateTravelAlertStream = rekeyedTrainTimeTableUpdateStream
                .process(TimeTableUpdateProcessor::new,
                        Constants.StateStores.ACTIVE_REQUESTS_STATE_STORE,
                        Constants.StateStores.EARLY_REQUESTS_STATE_STORE,
                        Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);

        KStream<String, TravelAlert> planeTimeTableUpdateTravelAlertStream = rekeyedPlaneTimeTableUpdateStream
                .process(TimeTableUpdateProcessor::new,
                        Constants.StateStores.ACTIVE_REQUESTS_STATE_STORE,
                        Constants.StateStores.EARLY_REQUESTS_STATE_STORE,
                        Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);

        KStream<String, TravelAlert> customerTravelRequestTravelAlertStream = rekeyedCustomerTravelRequestStream
                .process(CustomerTravelRequestProcessor::new,
                        Constants.StateStores.ACTIVE_REQUESTS_STATE_STORE,
                        Constants.StateStores.EARLY_REQUESTS_STATE_STORE,
                        Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);

        // Merge everything
        trainTimeTableUpdateTravelAlertStream.merge(planeTimeTableUpdateTravelAlertStream)
                .merge(customerTravelRequestTravelAlertStream)
                .to(GlobalConstants.Topics.TRAVEL_ALERTS_TOPIC,
                        Produced.with(Serdes.String(), Constants.Serdes.TRAVEL_ALERTS_SERDE));

        return getStreamsBuilder().build();
    }
}
