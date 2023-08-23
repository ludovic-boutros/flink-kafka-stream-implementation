package org.lboutros.traveloptimizer.kstreams.topology;

import lombok.Builder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.lboutros.traveloptimizer.kstreams.configuration.Constants;
import org.lboutros.traveloptimizer.kstreams.processor.TrainTimeTableUpdateProcessor;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;

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
        // TODO
        // Init
        Map<String, Object> serdeConfiguration = toMap(getStreamsConfiguration());

        Constants.Serdes.TRAVEL_ALERTS_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.PLANE_TIME_UPDATE_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.TRAIN_TIME_UPDATE_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE.configure(serdeConfiguration, false);

        // Get the customer travel request stream
        final KStream<String, CustomerTravelRequest> customerTravelRequestStream = getStreamsBuilder().stream(
                Constants.Topics.CUSTOMER_TRAVEL_REQUEST_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE));

        // Get the customer travel request stream
        final KStream<String, TrainTimeTableUpdate> trainTimeTableUpdateStream = getStreamsBuilder().stream(
                Constants.Topics.TRAIN_TIME_UPDATE_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.TRAIN_TIME_UPDATE_SERDE));

        // Get the customer travel request stream
        final KStream<String, PlaneTimeTableUpdate> planeTimeTableUpdateStream = getStreamsBuilder().stream(
                Constants.Topics.PLANE_TIME_UPDATE_TOPIC,
                Consumed.with(Serdes.String(), Constants.Serdes.PLANE_TIME_UPDATE_SERDE));

        // Now select key and repartition all streams
        KStream<String, CustomerTravelRequest> rekeyedCustomerTravelRequestStream = customerTravelRequestStream
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE)
                        .withName(Constants.Topics.REKEYED_CUSTOMER_TRAVEL_REQUEST_TOPIC));

        KStream<String, TrainTimeTableUpdate> rekeyedTrainTimeTableUpdateStream = trainTimeTableUpdateStream
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), Constants.Serdes.TRAIN_TIME_UPDATE_SERDE)
                        .withName(Constants.Topics.REKEYED_TRAIN_TIME_UPDATE_TOPIC));

        KStream<String, PlaneTimeTableUpdate> rekeyedPlaneTimeTableUpdateStream = planeTimeTableUpdateStream
                .selectKey((k, v) -> v.getDepartureLocation() + '#' + v.getArrivalLocation())
                .repartition(Repartitioned.with(Serdes.String(), Constants.Serdes.PLANE_TIME_UPDATE_SERDE)
                        .withName(Constants.Topics.REKEYED_PLANE_TIME_UPDATE_TOPIC));

        // Process each stream and produce travel alerts
        rekeyedTrainTimeTableUpdateStream.process(TrainTimeTableUpdateProcessor::new);

        // Merge everything

        return getStreamsBuilder().build();
    }
}
