package org.lboutros.traveloptimizer.flink.datagen;

import org.lboutros.traveloptimizer.model.*;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

public class DataGenerator {
    public static final int MAX_AIRPORT_COUNT = 1;
    private static final Random RANDOM = new Random(System.currentTimeMillis());

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

    private static ZonedDateTime generateDepartureTime(int timeSlot) {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
        ZonedDateTime departureTime = now
                .withHour(0)
                .withMinute(0)
                .withSecond(0)
                .withNano(0)
                .plus(timeSlot, ChronoUnit.HOURS)
                .plusSeconds(RANDOM.nextInt(3600));

        if (departureTime.isBefore(now)) {
            departureTime = departureTime.plusDays(1);
        }

        return departureTime;
    }

    private static ZonedDateTime generateArrivalTime(ZonedDateTime departure) {
        return departure
                .plusHours(RANDOM.nextInt(15) + 1)
                .plusMinutes(RANDOM.nextInt(60));
    }

    public static PlaneTimeTableUpdate generatePlaneData() {
        return generatePlaneData(null);
    }

    public static PlaneTimeTableUpdate generatePlaneData(Integer slot) {
        String departureLocation = generateAirportCode(MAX_AIRPORT_COUNT);
        String arrivalLocation = generateAirportCode(MAX_AIRPORT_COUNT, departureLocation);
        int timeSlot = (slot == null ? RANDOM.nextInt(24) : slot);
        ZonedDateTime departureTime = generateDepartureTime(timeSlot);
        ZonedDateTime arrivalTime = generateArrivalTime(departureTime);

        PlaneTimeTableUpdate flightData = new PlaneTimeTableUpdate();

        flightData.setUpdateId(UUID.randomUUID().toString());
        flightData.setTravelId(computeTravelId(departureLocation, arrivalLocation, timeSlot, TravelType.PLANE));
        flightData.setDepartureLocation(departureLocation);
        flightData.setArrivalLocation(arrivalLocation);
        flightData.setDepartureTime(departureTime);
        flightData.setArrivalTime(arrivalTime);

        return flightData;
    }

    public static TrainTimeTableUpdate generateTrainData() {
        return generateTrainData(null);
    }

    public static TrainTimeTableUpdate generateTrainData(Integer slot) {
        String departureLocation = generateAirportCode(MAX_AIRPORT_COUNT);
        String arrivalLocation = generateAirportCode(MAX_AIRPORT_COUNT, departureLocation);
        int timeSlot = (slot == null ? RANDOM.nextInt(24) : slot);
        ZonedDateTime departureTime = generateDepartureTime(timeSlot);
        ZonedDateTime arrivalTime = generateArrivalTime(departureTime);

        TrainTimeTableUpdate trainData = new TrainTimeTableUpdate();

        trainData.setUpdateId(UUID.randomUUID().toString());
        trainData.setTravelId(computeTravelId(departureLocation, arrivalLocation, timeSlot, TravelType.TRAIN));
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

    public static Departure generateDepartureData() {
        String departureLocation = generateAirportCode(MAX_AIRPORT_COUNT);
        String arrivalLocation = generateAirportCode(MAX_AIRPORT_COUNT, departureLocation);
        int timeSlot = RANDOM.nextInt(24);
        ZonedDateTime departureTime = generateDepartureTime(timeSlot);
        ZonedDateTime arrivalTime = generateArrivalTime(departureTime);

        Departure departureData = new Departure();

        departureData.setTravelId(computeTravelId(departureLocation, arrivalLocation, timeSlot, RANDOM.nextBoolean() ? TravelType.TRAIN : TravelType.PLANE));
        departureData.setEffectiveDepartureTime(departureTime);
        departureData.setExpectedArrivalTime(arrivalTime);
        departureData.setDepartureLocation(departureLocation);
        departureData.setArrivalLocation(arrivalLocation);
        departureData.setId(UUID.randomUUID().toString());

        return departureData;
    }

    private static String computeTravelId(String departureLocation, String arrivalLocation, int timeSlot, TravelType type) {
        return departureLocation + '#' + arrivalLocation + '#' + timeSlot + '_' + type;
    }
}
