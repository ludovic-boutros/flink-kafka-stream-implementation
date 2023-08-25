package org.lboutros.traveloptimizer.kstreams.topologies.businessrules;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.lboutros.traveloptimizer.kstreams.topologies.models.TimeTableEntry;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.Departure;
import org.lboutros.traveloptimizer.model.TravelAlert;

import java.util.ArrayList;
import java.util.List;

import static org.lboutros.traveloptimizer.kstreams.topologies.models.Utils.*;

public class EventManagement {
    private static final Serde<String> PREFIX_SERDE = Serdes.String();

    public static List<TravelAlert> whenATimeTableUpdateArose(TimeTableEntry timeUpdate,
                                                              KeyValueStore<String, TravelAlert> requestState,
                                                              KeyValueStore<String, TimeTableEntry> timeTableState,
                                                              KeyValueStore<String, CustomerTravelRequest> earlyRequestState) {

        var result = new ArrayList<TravelAlert>();

        timeTableState.put(timeUpdate.getTravelId(), timeUpdate);

        result.addAll(handleImpactedActiveTravels(requestState, timeTableState, timeUpdate));
        result.addAll(handleEarlyRequests(requestState, earlyRequestState, timeUpdate));

        return result;
    }

    // We don't manage potential timetable state zombie events due to the race condition between departure events and time table update events.
    // We just filter out updates with a departure to close from the current time.
    public static List<TravelAlert> whenADepartsOccurred(Departure departure, KeyValueStore<String, TravelAlert> requestState, KeyValueStore<String, TimeTableEntry> timeTableState) {
        List<TravelAlert> result = new ArrayList<>();

        timeTableState.delete(departure.getTravelId());

        try (KeyValueIterator<String, TravelAlert> iterator = requestState.prefixScan(departure.getTravelId(), PREFIX_SERDE.serializer())) {
            iterator.forEachRemaining(v -> {
                requestState.delete(v.key);

                v.value.setReason(TravelAlert.ReasonCodes.DEPARTED);
                v.value.setUpdateId(departure.getId());
                result.add(v.value);
            });
        }

        return result;
    }

    public static List<TravelAlert> whenACustomerRequestArose(CustomerTravelRequest request,
                                                              KeyValueStore<String, TravelAlert> requestState,
                                                              KeyValueStore<String, TimeTableEntry> timeTableState,
                                                              KeyValueStore<String, CustomerTravelRequest> earlyRequestState) {

        var result = new ArrayList<TravelAlert>();
        var newTravelAlert = TravelAlert.fromRequest(request);
        String departureLocation = request.getDepartureLocation();
        String arrivalLocation = request.getArrivalLocation();

        // Lookup for matching timetable entry using partition key 'prefix scan'
        TimeTableEntry optimalTravel = getOptimal(departureLocation, arrivalLocation, timeTableState);
        // Apply business rules
        if (optimalTravel != null) {
            updateAlert(newTravelAlert, optimalTravel, null);

            requestState.put(getRequestStateKey(optimalTravel.getTravelId(), request.getId()), newTravelAlert);

            // Forward the result
            result.add(newTravelAlert);
        } else {
            earlyRequestState.put(getEarlyRequestStateKey(departureLocation, arrivalLocation, request.getId()), request);
        }

        return result;
    }

    private static List<TravelAlert> handleImpactedActiveTravels(
            KeyValueStore<String, TravelAlert> requestState,
            KeyValueStore<String, TimeTableEntry> timeTableState,
            TimeTableEntry timeUpdate) {
        String departureLocation = timeUpdate.getDepartureLocation();
        String arrivalLocation = timeUpdate.getArrivalLocation();

        String stateStoreSearchPrefix = getStateStoreSearchPrefix(departureLocation, arrivalLocation);

        var result = new ArrayList<TravelAlert>();
        // Lookup for impacted travels
        TimeTableEntry optimal = getOptimal(departureLocation, arrivalLocation, timeTableState);

        // Get request alert for this customer travel request
        try (KeyValueIterator<String, TravelAlert> requestAlerts = requestState.prefixScan(stateStoreSearchPrefix, PREFIX_SERDE.serializer())) {
            while (requestAlerts.hasNext()) {
                TravelAlert travelAlert = requestAlerts.next().value;

                // Remove current alert
                String currentRequestAlertKey = getRequestStateKey(travelAlert.getTravelId(), travelAlert.getId());
                requestState.delete(currentRequestAlertKey);

                updateAlert(travelAlert, optimal, timeUpdate.getUpdateId());

                // Update Stores
                String newRequestAlertKey = getRequestStateKey(travelAlert.getTravelId(), travelAlert.getId());
                requestState.put(newRequestAlertKey, travelAlert);

                // Collect
                result.add(travelAlert); // the new alert
            }
        }

        return result;
    }

    private static List<TravelAlert> handleEarlyRequests(KeyValueStore<String, TravelAlert> requestState, KeyValueStore<String, CustomerTravelRequest> earlyRequestState, TimeTableEntry timeUpdate) {
        List<TravelAlert> result = new ArrayList<>();

        // Check for early requests
        String departureLocation = timeUpdate.getDepartureLocation();
        String arrivalLocation = timeUpdate.getArrivalLocation();

        String stateStoreSearchPrefix = getStateStoreSearchPrefix(departureLocation, arrivalLocation);

        try (KeyValueIterator<String, CustomerTravelRequest> iterator = earlyRequestState.prefixScan(stateStoreSearchPrefix, PREFIX_SERDE.serializer())) {
            while (iterator.hasNext()) {
                KeyValue<String, CustomerTravelRequest> next = iterator.next();

                earlyRequestState.delete(next.key);
                // Update Request Store
                var newAlert = updateAlert(TravelAlert.fromRequest(next.value), timeUpdate, timeUpdate.getUpdateId());
                newAlert.setUpdateId(timeUpdate.getUpdateId());

                String newRequestAlertKey = getRequestStateKey(newAlert.getTravelId(), newAlert.getId());
                requestState.put(newRequestAlertKey, newAlert);

                result.add(newAlert);
            }
        }

        return result;
    }

    private static TimeTableEntry getOptimal(String departure,
                                             String arrival,
                                             KeyValueStore<String, TimeTableEntry> timeTableState) {
        // Lookup for matching timetable entry using partition key 'prefix scan'
        String stateStoreSearchPrefix = getStateStoreSearchPrefix(departure, arrival);

        TimeTableEntry newOptimal = null;

        // Get new optimal travel for impacted requests
        try (KeyValueIterator<String, TimeTableEntry> availableConnections = timeTableState.prefixScan(stateStoreSearchPrefix, PREFIX_SERDE.serializer())) {
            while (availableConnections.hasNext()) {
                KeyValue<String, TimeTableEntry> next = availableConnections.next();

                if (newOptimal == null || next.value.getArrivalTime().isBefore(newOptimal.getArrivalTime())) {
                    newOptimal = next.value;
                }
            }
        }

        return newOptimal;
    }
}
