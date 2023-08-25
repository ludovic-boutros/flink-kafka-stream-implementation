package org.lboutros.traveloptimizer.flink.jobs;

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.lboutros.traveloptimizer.flink.datagen.TimeSerializers;
import org.lboutros.traveloptimizer.model.*;
import org.lboutros.traveloptimizer.model.generator.DataGenerator;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TravelOptimizerJobIntegrationTest {

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
    WatermarkStrategy<Departure> departureStrategy;
    DataStream.Collector<TravelAlert> collector;


    private void assertContains(DataStream.Collector<TravelAlert> collector, List<TravelAlert> expected) {
        List<TravelAlert> actual = new ArrayList<>();
        collector.getOutput().forEachRemaining(actual::add);

        assertEquals(expected.size(), actual.size());

        assertTrue(actual.containsAll(expected));
    }

    @SneakyThrows
    @BeforeEach
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().registerTypeWithKryoSerializer(ZonedDateTime.class, TimeSerializers.ZonedDateTimeSerializer.class);
        requestStrategy = WatermarkStrategy.noWatermarks();

        trainStrategy = WatermarkStrategy.noWatermarks();

        planeStrategy = WatermarkStrategy.noWatermarks();

        departureStrategy = WatermarkStrategy.noWatermarks();

        collector = new DataStream.Collector<>();

    }

    @Ignore
    @Test
    public void shouldGenerateAnAlertWhenRequestIsReceived() throws Exception {

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

        Departure departure = DataGenerator.generateDepartureData();
        departure.setDepartureLocation("Y");
        departure.setArrivalLocation("Z");

        TravelAlert expected = new TravelAlert();
        expected.setId(request.getId());
        expected.setDepartureLocation(request.getDepartureLocation());
        expected.setArrivalLocation(request.getArrivalLocation());
        expected.setDepartureTime(trainUpdate.getDepartureTime());
        expected.setArrivalTime(trainUpdate.getArrivalTime());
        expected.setTravelType(TravelType.TRAIN);
        expected.setTravelId(trainUpdate.getTravelId());


        DataStream<TrainTimeTableUpdate> trainStream = env.fromElements(trainUpdate)
                .assignTimestampsAndWatermarks(trainStrategy);

        DataStream<PlaneTimeTableUpdate> planeStream = env.fromElements(planeUpdate)
                .assignTimestampsAndWatermarks(planeStrategy);

        DataStream<CustomerTravelRequest> requestStream = env.fromElements(request)
                .assignTimestampsAndWatermarks(requestStrategy);

        DataStream<Departure> departureStream = env.fromElements(departure)
                .assignTimestampsAndWatermarks(departureStrategy);

        // When
        TravelOptimizerJob
                .defineWorkflow(planeStream, trainStream, requestStream, departureStream)
                .collectAsync(collector);

        env.executeAsync();

        // Then
        expected.setDepartureTime(trainUpdate.getDepartureTime());
        expected.setArrivalTime(trainUpdate.getArrivalTime());

        assertContains(collector, List.of(expected));
    }


}
