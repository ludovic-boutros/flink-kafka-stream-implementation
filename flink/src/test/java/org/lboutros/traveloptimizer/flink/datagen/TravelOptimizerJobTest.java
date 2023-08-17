package org.lboutros.traveloptimizer.flink.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.lboutros.traveloptimizer.flink.jobs.TravelOptimizerJob;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.UnionEnvelope;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TravelAlert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TravelOptimizerJobTest {

    static final MiniClusterResourceConfiguration miniClusterConfig = new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(1)
            .setNumberTaskManagers(1)
            .build();
    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(miniClusterConfig);
    StreamExecutionEnvironment env;
    WatermarkStrategy<CustomerTravelRequest> requestStrategy;
    WatermarkStrategy<TrainTimeTableUpdate> trainStrategy;
    WatermarkStrategy<UnionEnvelope> unionStrategy;
    WatermarkStrategy<PlaneTimeTableUpdate> planeStrategy;
    DataStream.Collector<TravelAlert> collector;

    private void assertContains(DataStream.Collector<TravelAlert> collector, List<TravelAlert> expected) {
        List<TravelAlert> actual = new ArrayList<>();
        collector.getOutput().forEachRemaining(actual::add);

        assertEquals(expected.size(), actual.size());

        assertTrue(actual.containsAll(expected));
    }

    public <T> JsonSerializationSchema<T> getJsonSerializerSchema() {
        return new JsonSerializationSchema<>(
                () -> new ObjectMapper()
                        .registerModule(new JavaTimeModule()));
    }

    @BeforeEach
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        requestStrategy = WatermarkStrategy.noWatermarks();

        trainStrategy = WatermarkStrategy
                .<TrainTimeTableUpdate>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis());

        planeStrategy = WatermarkStrategy
                .<PlaneTimeTableUpdate>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis());

        unionStrategy = WatermarkStrategy.noWatermarks();

        collector = new DataStream.Collector<>();
    }

    @Test
    public void defineWorkflow_shouldConvertFlightDataToUserStatistics() throws Exception {

        // Given
        CustomerTravelRequest request = DataGenerator.generateCustomerTravelRequestData();
        request.setDepartureLocation("ATL");
        request.setArrivalLocation("DFW");

        TrainTimeTableUpdate trainUpdate = DataGenerator.generateTrainData();
        trainUpdate.setDepartureLocation("ATL");
        trainUpdate.setArrivalLocation("DFW");

        PlaneTimeTableUpdate planeUpdate = DataGenerator.generatePlaneData();
        planeUpdate.setDepartureLocation("X");
        planeUpdate.setArrivalLocation("Y");

        TravelAlert expected = new TravelAlert();
        expected.setId(request.getId());
        expected.setDepartureLocation(request.getDepartureLocation());
        expected.setArrivalLocation(request.getArrivalLocation());
        expected.setDepartureTime(trainUpdate.getDepartureTime());
        expected.setArrivalTime(trainUpdate.getArrivalTime());

        List<UnionEnvelope> elements = List.of(
                UnionEnvelope.fromTrainTimeTableUpdate(trainUpdate),
                UnionEnvelope.fromPlaneTimeTableUpdate(planeUpdate),
                UnionEnvelope.fromCustomerTravelRequest(request)
        );

        DataStream<UnionEnvelope> unionStream = env.fromCollection(elements)
                .assignTimestampsAndWatermarks(unionStrategy);

        DataStream<TrainTimeTableUpdate> trainStream = unionStream
                .filter(e -> e.getTrainTimeTableUpdate() != null)
                .map(UnionEnvelope::getTrainTimeTableUpdate);

        DataStream<PlaneTimeTableUpdate> planeStream = unionStream
                .filter(e -> e.getPlaneTimeTableUpdate() != null)
                .map(UnionEnvelope::getPlaneTimeTableUpdate);

        DataStream<CustomerTravelRequest> requestStream = unionStream
                .filter(e -> e.getCustomerTravelRequest() != null)
                .map(UnionEnvelope::getCustomerTravelRequest)
                // Wait a bit for travel updates !! Ugggggly...
                .map(e -> {
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                    return e;
                });

        // When
        TravelOptimizerJob
                .defineWorkflow(planeStream, trainStream, requestStream)
                .collectAsync(collector);

        env.executeAsync();

        // Then
        assertContains(collector, List.of(expected));
    }
}
