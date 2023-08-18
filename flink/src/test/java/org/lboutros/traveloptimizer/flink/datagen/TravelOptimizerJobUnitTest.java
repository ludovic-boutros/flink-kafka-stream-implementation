package org.lboutros.traveloptimizer.flink.datagen;

import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.UnionEnvelope;
import org.lboutros.traveloptimizer.flink.processfunctions.OptimizerFunction;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TravelAlert;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TravelOptimizerJobUnitTest {
    KeyedOneInputStreamOperatorTestHarness<String, UnionEnvelope, TravelAlert> optimizerTestHarness;

    private void assertContains(List<TravelAlert> list, List<TravelAlert> expected) {
        assertEquals(expected.size(), list.size());
        assertTrue(list.containsAll(expected));
    }

    public <T> JsonSerializationSchema<T> getJsonSerializerSchema() {
        return new JsonSerializationSchema<>(
                () -> new ObjectMapper()
                        .registerModule(new JavaTimeModule()));
    }

    @SneakyThrows
    @BeforeEach
    public void setup() {

        var processOperatorToTest = new KeyedProcessOperator<>(new OptimizerFunction());
        optimizerTestHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(processOperatorToTest, UnionEnvelope::getPartitionKey, Types.STRING);

        optimizerTestHarness.setup();
        optimizerTestHarness.open();

    }

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
        expected.setTravelId(trainUpdate.getId());

        List<UnionEnvelope> elements = List.of(
                UnionEnvelope.fromTrainTimeTableUpdate(trainUpdate),
                UnionEnvelope.fromPlaneTimeTableUpdate(planeUpdate),
                UnionEnvelope.fromCustomerTravelRequest(request)
        );

        // When
        optimizerTestHarness.processElement(elements.get(0), 1L); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(elements.get(1), 1L);
        optimizerTestHarness.processElement(elements.get(2), 1L);

        // Then
        assertContains(optimizerTestHarness.getRecordOutput().stream().map(StreamRecord::getValue).collect(Collectors.toList()), List.of(expected));
    }

    @Test
    public void shouldNotGenerateAnAlertWithoutTimeTableEntry() throws Exception {

        // Given
        CustomerTravelRequest request = DataGenerator.generateCustomerTravelRequestData();
        request.setDepartureLocation("ATL");
        request.setArrivalLocation("DFW");


        List<UnionEnvelope> elements = List.of(
                UnionEnvelope.fromCustomerTravelRequest(request)
        );

        // When
        optimizerTestHarness.processElement(elements.get(0), 1L);

        // Then
        assertTrue(optimizerTestHarness.getRecordOutput().isEmpty());
    }

    @Test
    public void shouldGenerateAnAlertWithLateTimeTableEntry() throws Exception {

        // Given
        CustomerTravelRequest request = DataGenerator.generateCustomerTravelRequestData();
        request.setDepartureLocation("ATL");
        request.setArrivalLocation("DFW");

        TrainTimeTableUpdate trainUpdate = DataGenerator.generateTrainData();
        trainUpdate.setDepartureLocation("ATL");
        trainUpdate.setArrivalLocation("DFW");

        TravelAlert expected = new TravelAlert();
        expected.setId(request.getId());
        expected.setDepartureLocation(request.getDepartureLocation());
        expected.setArrivalLocation(request.getArrivalLocation());
        expected.setDepartureTime(trainUpdate.getDepartureTime());
        expected.setArrivalTime(trainUpdate.getArrivalTime());
        expected.setTravelId(trainUpdate.getId());

        List<UnionEnvelope> elements = List.of(
                UnionEnvelope.fromCustomerTravelRequest(request),
                UnionEnvelope.fromTrainTimeTableUpdate(trainUpdate)
        );

        // When
        optimizerTestHarness.processElement(elements.get(0), 1L); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(elements.get(1), 1L);

        // Then
        assertContains(optimizerTestHarness.getRecordOutput().stream().map(StreamRecord::getValue).collect(Collectors.toList()), List.of(expected));
    }
}
