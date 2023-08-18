package org.lboutros.traveloptimizer.flink.datagen;

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.Ignore;
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

    @SneakyThrows
    @BeforeEach
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        requestStrategy = WatermarkStrategy.noWatermarks();

        trainStrategy = WatermarkStrategy.noWatermarks();

        planeStrategy = WatermarkStrategy.noWatermarks();

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

        TravelAlert expected = new TravelAlert();
        expected.setId(request.getId());
        expected.setDepartureLocation(request.getDepartureLocation());
        expected.setArrivalLocation(request.getArrivalLocation());
        expected.setDepartureTime(trainUpdate.getDepartureTime());
        expected.setArrivalTime(trainUpdate.getArrivalTime());


        DataStream<TrainTimeTableUpdate> trainStream = env.fromElements(trainUpdate)
                .assignTimestampsAndWatermarks(trainStrategy);

        DataStream<PlaneTimeTableUpdate> planeStream = env.fromElements(planeUpdate)
                .assignTimestampsAndWatermarks(planeStrategy);

        DataStream<CustomerTravelRequest> requestStream = env.fromElements(request)
                .assignTimestampsAndWatermarks(requestStrategy);

        // When
        TravelOptimizerJob
                .defineWorkflow(planeStream, trainStream, requestStream)
                .collectAsync(collector);

        env.executeAsync();

        // Then
        //assertContains(collector, List.of(expected));
    }


}
