package org.lboutros.traveloptimizer.kstreams.topologies;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lboutros.traveloptimizer.GlobalConstants;
import org.lboutros.traveloptimizer.kstreams.configuration.Constants;
import org.lboutros.traveloptimizer.model.*;
import org.lboutros.traveloptimizer.model.generator.DataGenerator;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.lboutros.traveloptimizer.kstreams.configuration.Utils.readConfiguration;
import static org.lboutros.traveloptimizer.kstreams.configuration.Utils.toMap;
import static org.lboutros.traveloptimizer.model.TravelAlert.ReasonCodes.DEPARTED;

class TravelOptimizerTopologySupplierTest {
    private final Random R = new Random();
    private final Serde<String> keySerde = Serdes.String();
    private TestInputTopic<String, TrainTimeTableUpdate> trainTimeTableUpdateInputTopic;
    private TestInputTopic<String, PlaneTimeTableUpdate> planeTimeTableUpdateInputTopic;
    private TestInputTopic<String, CustomerTravelRequest> customerTravelRequestInputTopic;
    private TestInputTopic<String, Departure> departureInputTopic;
    private TestOutputTopic<String, TravelAlert> travelAlertOutputTopic;
    private TopologyTestDriver testDriver;

    private void assertContains(List<TravelAlert> list, List<TravelAlert> expected) {
        assertEquals(expected.size(), list.size());
        assertTrue(list.containsAll(expected));
    }

    @BeforeEach
    public void setup() throws ConfigurationException {
        Properties streamsConfiguration = readConfiguration("test.properties");
        // Init serdes
        Map<String, Object> serdeConfiguration = toMap(streamsConfiguration);

        Constants.Serdes.TRAVEL_ALERTS_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.PLANE_TIME_UPDATE_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.TRAIN_TIME_UPDATE_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.TIME_UPDATE_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE.configure(serdeConfiguration, false);
        Constants.Serdes.DEPARTURE_SERDE.configure(serdeConfiguration, false);

        // Init test driver
        testDriver = new TopologyTestDriver(new TravelOptimizerTopologySupplier(streamsConfiguration).get(), streamsConfiguration, Instant.EPOCH);

        // Setup test topics
        trainTimeTableUpdateInputTopic = testDriver.createInputTopic(GlobalConstants.Topics.TRAIN_TIME_UPDATE_TOPIC,
                keySerde.serializer(),
                Constants.Serdes.TRAIN_TIME_UPDATE_SERDE.serializer(),
                Instant.EPOCH,
                Duration.ZERO
        );

        planeTimeTableUpdateInputTopic = testDriver.createInputTopic(GlobalConstants.Topics.PLANE_TIME_UPDATE_TOPIC,
                keySerde.serializer(),
                Constants.Serdes.PLANE_TIME_UPDATE_SERDE.serializer(),
                Instant.EPOCH,
                Duration.ZERO
        );

        customerTravelRequestInputTopic = testDriver.createInputTopic(GlobalConstants.Topics.CUSTOMER_TRAVEL_REQUEST_TOPIC,
                keySerde.serializer(),
                Constants.Serdes.CUSTOMER_TRAVEL_REQUEST_SERDE.serializer(),
                Instant.EPOCH,
                Duration.ZERO
        );

        departureInputTopic = testDriver.createInputTopic(GlobalConstants.Topics.DEPARTURE_TOPIC,
                keySerde.serializer(),
                Constants.Serdes.DEPARTURE_SERDE.serializer(),
                Instant.EPOCH,
                Duration.ZERO
        );

        travelAlertOutputTopic = testDriver.createOutputTopic(GlobalConstants.Topics.TRAVEL_ALERTS_TOPIC,
                keySerde.deserializer(),
                Constants.Serdes.TRAVEL_ALERTS_SERDE.deserializer()
        );
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }


    @Test
    public void shouldGenerateAnAlertWhenRequestIsReceived() {
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
        trainTimeTableUpdateInputTopic.pipeInput(trainUpdate.getUpdateId(), trainUpdate);
        customerTravelRequestInputTopic.pipeInput(request.getId(), request);
        planeTimeTableUpdateInputTopic.pipeInput(planeUpdate.getUpdateId(), planeUpdate);
        planeTimeTableUpdateInputTopic.pipeInput(planeUpdateXY.getUpdateId(), planeUpdateXY);

        // Then
        assertContains(travelAlertOutputTopic.readValuesToList(), List.of(expectedTrainAlert, expectedPlaneAlert));
    }

    @Test
    public void shouldNotGenerateAnAlertWithoutTimeTableEntry() {

        // Given
        CustomerTravelRequest request = DataGenerator.generateCustomerTravelRequestData();
        request.setDepartureLocation("ATL");
        request.setArrivalLocation("DFW");

        // When
        customerTravelRequestInputTopic.pipeInput(request.getId(), request);

        // Then
        assertContains(travelAlertOutputTopic.readValuesToList(), List.of());
    }

    @Test
    public void shouldGenerateAnAlertWithLateTimeTableEntry() {

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

        // When
        customerTravelRequestInputTopic.pipeInput(request.getId(), request);
        trainTimeTableUpdateInputTopic.pipeInput(trainUpdate.getUpdateId(), trainUpdate);

        // Then
        assertContains(travelAlertOutputTopic.readValuesToList(), List.of(expected));
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
        customerTravelRequestInputTopic.pipeInput(request.getId(), request);
        trainTimeTableUpdateInputTopic.pipeInput(trainUpdate.getUpdateId(), trainUpdate);
        departureInputTopic.pipeInput(departure.getId(), departure);

        // Then
        assertContains(travelAlertOutputTopic.readValuesToList(),
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
        trainTimeTableUpdateInputTopic.pipeInput(trainUpdate.getUpdateId(), trainUpdate);
        departureInputTopic.pipeInput(departure.getId(), departure);

        // Then
        assertContains(travelAlertOutputTopic.readValuesToList(),
                List.of());
    }

    @Test
    public void shouldNotGenerateAnAlertWhenEmptyStateEntryForTravel() throws Exception {
        // Given
        Departure departure = DataGenerator.generateDepartureData();

        // When
        departureInputTopic.pipeInput(departure.getId(), departure);

        // Then
        assertContains(travelAlertOutputTopic.readValuesToList(),
                List.of());
    }
}