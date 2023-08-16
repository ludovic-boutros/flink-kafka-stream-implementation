package org.lboutros.traveloptimizer.flink.datagen;

import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataGenerator {
    public static final int MAX_AIRPORT_COUNT = 4;
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final List<String> USERS = Stream
            .generate(() -> generateString(5) + "@email.com")
            .limit(100)
            .collect(Collectors.toList());

    public static String generateAirportCode(int maxAirportCount) {
        return generateAirportCode(maxAirportCount, null);
    }

    public static String generateAirportCode(int maxAirportCount, String alreadySelectedAirport) {
        String[] airports = new String[]{
                "ATL", "DFW", "DEN", "ORD", "LAX", "CLT", "MCO", "LAS", "PHX", "MIA",
                "SEA", "IAH", "JFK", "EWR", "FLL", "MSP", "SFO", "DTW", "BOS", "SLC",
                "PHL", "BWI", "TPA", "SAN", "LGA", "MDW", "BNA", "IAD", "DCA", "AUS"
        };

        List<String> filteredAirportList = Arrays.stream(airports)
                .filter(a -> alreadySelectedAirport == null || !alreadySelectedAirport.equals(a))
                .collect(Collectors.toList());

        String airport = filteredAirportList.get(RANDOM.nextInt(Math.min(maxAirportCount, filteredAirportList.size())));

        assert (!airport.equals(alreadySelectedAirport));

        return airport;
    }

    private static String generateString(int size) {
        final String alphaString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        StringBuilder sb = new StringBuilder(size);

        for (int i = 0; i < size; i++) {
            final int index = RANDOM.nextInt(alphaString.length());
            sb.append(alphaString.charAt(index));
        }

        return sb.toString();
    }

    private static String generateEmail() {
        return USERS.get(RANDOM.nextInt(USERS.size()));
    }

    private static ZonedDateTime generateDepartureTime(int timeSlot) {
        return LocalDate.now()
                .atTime(timeSlot, 0, 0, 0)
                .plusSeconds(RANDOM.nextInt(3600))
                .atZone(ZoneId.of("UTC"));
    }

    private static ZonedDateTime generateArrivalTime(ZonedDateTime departure) {
        return departure
                .plusHours(RANDOM.nextInt(15) + 1)
                .plusMinutes(RANDOM.nextInt(60));
    }

    public static PlaneTimeTableUpdate generatePlaneData() {
        String departureLocation = generateAirportCode(MAX_AIRPORT_COUNT);
        String arrivalLocation = generateAirportCode(MAX_AIRPORT_COUNT, departureLocation);
        int timeSlot = RANDOM.nextInt(24);
        ZonedDateTime departureTime = generateDepartureTime(timeSlot);
        ZonedDateTime arrivalTime = generateArrivalTime(departureTime);

        PlaneTimeTableUpdate flightData = new PlaneTimeTableUpdate();

        flightData.setId(departureLocation + '#' + arrivalLocation + '_' + timeSlot);
        flightData.setDepartureLocation(departureLocation);
        flightData.setArrivalLocation(arrivalLocation);
        flightData.setDepartureTime(departureTime);
        flightData.setArrivalTime(arrivalTime);

        return flightData;
    }

    public static TrainTimeTableUpdate generateTrainData() {
        String departureLocation = generateAirportCode(MAX_AIRPORT_COUNT);
        String arrivalLocation = generateAirportCode(MAX_AIRPORT_COUNT, departureLocation);
        int timeSlot = RANDOM.nextInt(24);
        ZonedDateTime departureTime = generateDepartureTime(timeSlot);
        ZonedDateTime arrivalTime = generateArrivalTime(departureTime);

        TrainTimeTableUpdate trainData = new TrainTimeTableUpdate();

        trainData.setId(departureLocation + '#' + arrivalLocation + '_' + timeSlot);
        trainData.setDepartureLocation(departureLocation);
        trainData.setArrivalLocation(arrivalLocation);
        trainData.setDepartureTime(departureTime);
        trainData.setArrivalTime(arrivalTime);

        return trainData;
    }

    public static CustomerTravelRequest generateCustomerTravelRequestData() {
        String departureLocation = generateAirportCode(MAX_AIRPORT_COUNT);
        String arrivalLocation = generateAirportCode(MAX_AIRPORT_COUNT, departureLocation);

        CustomerTravelRequest customerTravelRequest = new CustomerTravelRequest();

        customerTravelRequest.setId(UUID.randomUUID().toString());
        customerTravelRequest.setDepartureLocation(departureLocation);
        customerTravelRequest.setArrivalLocation(arrivalLocation);
        
        return customerTravelRequest;
    }
}
