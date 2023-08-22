package org.lboutros.traveloptimizer.flink.jobs;

import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lboutros.traveloptimizer.flink.datagen.DataGenerator;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.UnionEnvelope;
import org.lboutros.traveloptimizer.flink.jobs.processfunctions.OptimizerFunction;
import org.lboutros.traveloptimizer.model.*;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.lboutros.traveloptimizer.model.TravelAlert.ReasonCodes.DEPARTED;

class TravelOptimizerJobUnitTest {
    private KeyedOneInputStreamOperatorTestHarness<String, UnionEnvelope, TravelAlert> optimizerTestHarness;
    private Random R = new Random();


    private void assertContains(List<TravelAlert> list, List<TravelAlert> expected) {
        assertEquals(expected.size(), list.size());
        assertTrue(list.containsAll(expected));
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

        int slot = R.nextInt(24);
        TrainTimeTableUpdate trainUpdate = DataGenerator.generateTrainData(slot);
        trainUpdate.setDepartureLocation("ATL");
        trainUpdate.setArrivalLocation("DFW");

        PlaneTimeTableUpdate planeUpdate = DataGenerator.generatePlaneData(slot);
        planeUpdate.setDepartureLocation("ATL");
        planeUpdate.setArrivalLocation("DFW");
        planeUpdate.setArrivalTime(trainUpdate.getArrivalTime().minusMinutes(2));

        PlaneTimeTableUpdate planeUpdateXY = DataGenerator.generatePlaneData();
        planeUpdateXY.setDepartureLocation("X");
        planeUpdateXY.setArrivalLocation("Y");

        TravelAlert expectedTrainAlert = new TravelAlert();
        expectedTrainAlert.setId(request.getId());
        expectedTrainAlert.setDepartureLocation(request.getDepartureLocation());
        expectedTrainAlert.setArrivalLocation(request.getArrivalLocation());
        expectedTrainAlert.setDepartureTime(trainUpdate.getDepartureTime());
        expectedTrainAlert.setArrivalTime(trainUpdate.getArrivalTime());
        expectedTrainAlert.setTravelId(trainUpdate.getTravelId());
        expectedTrainAlert.setUpdateId(null);
        expectedTrainAlert.setTravelType(TravelType.TRAIN);

        TravelAlert expectedPlaneAlert = new TravelAlert();
        expectedPlaneAlert.setId(request.getId());
        expectedPlaneAlert.setDepartureLocation(request.getDepartureLocation());
        expectedPlaneAlert.setArrivalLocation(request.getArrivalLocation());
        expectedPlaneAlert.setDepartureTime(planeUpdate.getDepartureTime());
        expectedPlaneAlert.setArrivalTime(planeUpdate.getArrivalTime());
        expectedPlaneAlert.setTravelId(planeUpdate.getTravelId());
        expectedPlaneAlert.setUpdateId(planeUpdate.getUpdateId());
        expectedPlaneAlert.setLastTravelId(trainUpdate.getTravelId());
        expectedPlaneAlert.setTravelType(TravelType.PLANE);

        List<UnionEnvelope> elements = List.of(
                UnionEnvelope.fromTrainTimeTableUpdate(trainUpdate),
                UnionEnvelope.fromCustomerTravelRequest(request),
                UnionEnvelope.fromPlaneTimeTableUpdate(planeUpdate),
                UnionEnvelope.fromPlaneTimeTableUpdate(planeUpdateXY)
        );

        // When
        optimizerTestHarness.processElement(elements.get(0), 1L); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(elements.get(1), 1L);
        optimizerTestHarness.processElement(elements.get(2), 1L);
        optimizerTestHarness.processElement(elements.get(3), 1L);

        // Then
        var actual = optimizerTestHarness.getRecordOutput().stream().map(StreamRecord::getValue).collect(Collectors.toList());
        assertContains(actual, List.of(expectedTrainAlert, expectedPlaneAlert));
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
        expected.setTravelId(trainUpdate.getTravelId());
        expected.setUpdateId(trainUpdate.getUpdateId());
        expected.setTravelType(TravelType.TRAIN);

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

    @Test
    public void shouldGenerateAnAlertWhenAnAssignedTravelDeparts() throws Exception {

        // Given
        CustomerTravelRequest request = DataGenerator.generateCustomerTravelRequestData();
        request.setDepartureLocation("ATL");
        request.setArrivalLocation("DFW");

        TrainTimeTableUpdate trainUpdate = DataGenerator.generateTrainData();
        trainUpdate.setDepartureLocation("ATL");
        trainUpdate.setArrivalLocation("DFW");

        Departure departure = DataGenerator.generateDepartureData();
        departure.setTravelId(trainUpdate.getTravelId());
        departure.setDepartureLocation("ATL");
        departure.setArrivalLocation("DFW");

        TravelAlert expectedTrainUpdateAlert = new TravelAlert();
        expectedTrainUpdateAlert.setId(request.getId());
        expectedTrainUpdateAlert.setDepartureLocation(request.getDepartureLocation());
        expectedTrainUpdateAlert.setArrivalLocation(request.getArrivalLocation());
        expectedTrainUpdateAlert.setDepartureTime(trainUpdate.getDepartureTime());
        expectedTrainUpdateAlert.setArrivalTime(trainUpdate.getArrivalTime());
        expectedTrainUpdateAlert.setTravelId(trainUpdate.getTravelId());
        expectedTrainUpdateAlert.setUpdateId(trainUpdate.getUpdateId());
        expectedTrainUpdateAlert.setTravelType(TravelType.TRAIN);

        TravelAlert expectedDepartedAlert = new TravelAlert();
        expectedDepartedAlert.setId(request.getId());
        expectedDepartedAlert.setDepartureLocation(request.getDepartureLocation());
        expectedDepartedAlert.setArrivalLocation(request.getArrivalLocation());
        expectedDepartedAlert.setDepartureTime(trainUpdate.getDepartureTime());
        expectedDepartedAlert.setArrivalTime(trainUpdate.getArrivalTime());
        expectedDepartedAlert.setTravelId(trainUpdate.getTravelId());
        expectedDepartedAlert.setTravelType(TravelType.TRAIN);
        expectedDepartedAlert.setUpdateId(departure.getId());
        expectedDepartedAlert.setReason(DEPARTED);

        List<UnionEnvelope> elements = List.of(
                UnionEnvelope.fromCustomerTravelRequest(request),
                UnionEnvelope.fromTrainTimeTableUpdate(trainUpdate),
                UnionEnvelope.fromDeparture(departure)
        );

        // When
        optimizerTestHarness.processElement(elements.get(0), 1L); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(elements.get(1), 1L);
        optimizerTestHarness.processElement(elements.get(2), 1L);

        // Then
        assertContains(optimizerTestHarness.getRecordOutput().stream()
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList()),
                List.of(expectedTrainUpdateAlert, expectedDepartedAlert));
    }

    @Test
    public void shouldNotGenerateAnAlertWhenNoAssignedTravelDeparts() throws Exception {

        // Given
        TrainTimeTableUpdate trainUpdate = DataGenerator.generateTrainData();
        trainUpdate.setDepartureLocation("ATL");
        trainUpdate.setArrivalLocation("DFW");

        Departure departure = DataGenerator.generateDepartureData();
        departure.setTravelId(trainUpdate.getTravelId());
        departure.setDepartureLocation("ATL");
        departure.setArrivalLocation("DFW");

        List<UnionEnvelope> elements = List.of(
                UnionEnvelope.fromTrainTimeTableUpdate(trainUpdate),
                UnionEnvelope.fromDeparture(departure)
        );

        // When
        optimizerTestHarness.processElement(elements.get(0), 1L); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(elements.get(1), 1L);

        // Then
        assertContains(optimizerTestHarness.getRecordOutput().stream()
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList()),
                List.of());
    }

    @Test
    public void shouldNotGenerateAnAlertWhenEmptyStateEntryForTravel() throws Exception {
        // Given
        Departure departure = DataGenerator.generateDepartureData();

        List<UnionEnvelope> elements = List.of(
                UnionEnvelope.fromDeparture(departure)
        );

        // When
        optimizerTestHarness.processElement(elements.get(0), 1L); // Timestamp is irrelevant #ignored

        // Then
        assertContains(optimizerTestHarness.getRecordOutput().stream()
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList()),
                List.of());
    }
}
