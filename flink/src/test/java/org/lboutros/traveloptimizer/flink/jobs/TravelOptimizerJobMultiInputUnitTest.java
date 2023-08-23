package org.lboutros.traveloptimizer.flink.jobs;

import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedMultiInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lboutros.traveloptimizer.flink.datagen.DataGenerator;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.Utils;
import org.lboutros.traveloptimizer.flink.jobs.operators.TravelOptimizerOperator;
import org.lboutros.traveloptimizer.model.*;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.lboutros.traveloptimizer.model.TravelAlert.ReasonCodes.DEPARTED;

class TravelOptimizerJobMultiInputUnitTest {
    private final Random R = new Random();
    private KeyedMultiInputStreamOperatorTestHarness<String, TravelAlert> optimizerTestHarness;

    private void assertContains(List<TravelAlert> list, List<TravelAlert> expected) {
        assertEquals(expected.size(), list.size());
        assertTrue(list.containsAll(expected));
    }

    @SneakyThrows
    @BeforeEach
    public void setup() {

        var processOperatorToTest = new TravelOptimizerOperator.TravelOptimizerOperatorFactory();


        optimizerTestHarness =
                new KeyedMultiInputStreamOperatorTestHarness<>(processOperatorToTest, BasicTypeInfo.STRING_TYPE_INFO);

        KeySelector<PlaneTimeTableUpdate, String> planeEventKeySelector = value -> Utils.getPartitionKey(value.getDepartureLocation(), value.getArrivalLocation());
        KeySelector<TrainTimeTableUpdate, String> trainEventKeySelector = value -> Utils.getPartitionKey(value.getDepartureLocation(), value.getArrivalLocation());
        KeySelector<CustomerTravelRequest, String> requestEventKeySelector = value -> Utils.getPartitionKey(value.getDepartureLocation(), value.getArrivalLocation());
        KeySelector<Departure, String> departureEventKeySelector = value -> Utils.getPartitionKey(value.getDepartureLocation(), value.getArrivalLocation());


        optimizerTestHarness.setKeySelector(TravelOptimizerOperator.InputIndexes.TRAIN_UPDATE - 1, trainEventKeySelector);
        optimizerTestHarness.setKeySelector(TravelOptimizerOperator.InputIndexes.PLANE_UPDATE - 1, planeEventKeySelector);
        optimizerTestHarness.setKeySelector(TravelOptimizerOperator.InputIndexes.REQUEST - 1, requestEventKeySelector);
        optimizerTestHarness.setKeySelector(TravelOptimizerOperator.InputIndexes.DEPARTURE - 1, departureEventKeySelector);

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

        // When
        optimizerTestHarness.processElement(TravelOptimizerOperator.InputIndexes.TRAIN_UPDATE - 1, new StreamRecord<>(trainUpdate)); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(TravelOptimizerOperator.InputIndexes.REQUEST - 1, new StreamRecord<>(request)); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(TravelOptimizerOperator.InputIndexes.PLANE_UPDATE - 1, new StreamRecord<>(planeUpdate)); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(TravelOptimizerOperator.InputIndexes.PLANE_UPDATE - 1, new StreamRecord<>(planeUpdateXY)); // Timestamp is irrelevant #ignored


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

        // When
        optimizerTestHarness.processElement(TravelOptimizerOperator.InputIndexes.REQUEST - 1, new StreamRecord<>(request)); // Timestamp is irrelevant #ignored


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

        optimizerTestHarness.processElement(getTestInputIndex(TravelOptimizerOperator.InputIndexes.REQUEST), new StreamRecord<>(request)); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(getTestInputIndex(TravelOptimizerOperator.InputIndexes.TRAIN_UPDATE), new StreamRecord<>(trainUpdate)); // Timestamp is irrelevant #ignored

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

        // When
        optimizerTestHarness.processElement(getTestInputIndex(TravelOptimizerOperator.InputIndexes.REQUEST), new StreamRecord<>(request)); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(getTestInputIndex(TravelOptimizerOperator.InputIndexes.TRAIN_UPDATE), new StreamRecord<>(trainUpdate)); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(getTestInputIndex(TravelOptimizerOperator.InputIndexes.DEPARTURE), new StreamRecord<>(departure)); // Timestamp is irrelevant #ignored

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

        // When
        optimizerTestHarness.processElement(getTestInputIndex(TravelOptimizerOperator.InputIndexes.TRAIN_UPDATE), new StreamRecord<>(trainUpdate)); // Timestamp is irrelevant #ignored
        optimizerTestHarness.processElement(getTestInputIndex(TravelOptimizerOperator.InputIndexes.DEPARTURE), new StreamRecord<>(departure)); // Timestamp is irrelevant #ignored

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

        // When
        optimizerTestHarness.processElement(getTestInputIndex(TravelOptimizerOperator.InputIndexes.DEPARTURE), new StreamRecord<>(departure)); // Timestamp is irrelevant #ignored

        // Then
        assertContains(optimizerTestHarness.getRecordOutput().stream()
                        .map(StreamRecord::getValue)
                        .collect(Collectors.toList()),
                List.of());
    }

    /**
     * Workaround for test input index being 0-based vs Real input being 1-based
     *
     * @param index
     * @return
     */
    private int getTestInputIndex(int index) {
        return index - 1;
    }
}
