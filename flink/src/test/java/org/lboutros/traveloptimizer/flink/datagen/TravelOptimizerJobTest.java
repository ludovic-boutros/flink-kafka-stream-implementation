package org.lboutros.traveloptimizer.flink.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.lboutros.traveloptimizer.flink.jobs.TravelOptimizerJob;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TravelAlert;

import java.util.ArrayList;
import java.util.List;

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
    WatermarkStrategy<PlaneTimeTableUpdate> planeStrategy;
    DataStream.Collector<TravelAlert> collector;

    private void assertContains(DataStream.Collector<TravelAlert> collector, List<TravelAlert> expected) {
        List<TravelAlert> actual = new ArrayList<>();
        collector.getOutput().forEachRemaining(actual::add);

        assertEquals(expected.size(), actual.size());

        assertTrue(actual.containsAll(expected));
    }

    @BeforeEach
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        requestStrategy = WatermarkStrategy
                .<CustomerTravelRequest>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis() + 10000);
        trainStrategy = WatermarkStrategy
                .<TrainTimeTableUpdate>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis() - 10000);
        planeStrategy = WatermarkStrategy
                .<PlaneTimeTableUpdate>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis());

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

        PlaneTimeTableUpdate plane = DataGenerator.generatePlaneData();
        trainUpdate.setDepartureLocation("X");
        trainUpdate.setArrivalLocation("Y");

        TravelAlert expected = new TravelAlert();
        expected.setId(request.getId());
        expected.setDepartureLocation(request.getDepartureLocation());
        expected.setArrivalLocation(request.getArrivalLocation());
        expected.setDepartureTime(trainUpdate.getDepartureTime());
        expected.setArrivalTime(trainUpdate.getArrivalTime());


        // When
        DataStream<TrainTimeTableUpdate> trainStream = env
                .fromElements(trainUpdate)
                .assignTimestampsAndWatermarks(trainStrategy);

        DataStream<CustomerTravelRequest> requestStream = env
                .fromElements(request)
                .assignTimestampsAndWatermarks(requestStrategy);

        DataStream<PlaneTimeTableUpdate> planeStream = env.fromElements(plane)
                .assignTimestampsAndWatermarks(planeStrategy);

        TravelOptimizerJob
                .defineWorkflow(planeStream, trainStream, requestStream)
                .collectAsync(collector);

        env.executeAsync();

        // Then

        assertContains(collector, List.of(expected));
    }


}
