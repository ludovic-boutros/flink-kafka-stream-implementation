package org.lboutros.traveloptimizer.flink.jobs.businessrules;

import org.apache.flink.api.common.state.MapState;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.AlertMap;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.RequestList;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.TimeTableEntry;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.TimeTableMap;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.Departure;
import org.lboutros.traveloptimizer.model.TravelAlert;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.lboutros.traveloptimizer.flink.jobs.internalmodels.Utils.*;

public class EventManagement {

    public static List<TravelAlert> whenATimeTableUpdateArose(TimeTableEntry timeUpdate, MapState<String, AlertMap> requestState, MapState<String, TimeTableMap> timeTableState, MapState<String, RequestList> earlyRequestState, String storageKey) throws Exception {

        var result = new ArrayList<TravelAlert>();
        var timeTableMap = getOrCreateStatestoreEntry(timeTableState, storageKey, TimeTableMap::new);

        // Upsert the new timetable entry
        timeTableMap.put(timeUpdate.getTravelId(), timeUpdate);

        result.addAll(handleImpactedActiveTravels(requestState, timeTableState, storageKey, timeUpdate, timeTableMap));
        result.addAll(handleEarlyRequests(requestState, earlyRequestState, storageKey, timeUpdate));
        return result;
    }

    // We don't manage potential timetable state zombie events due to the race condition between departure events and time table update events.
    // We just filter out updates with a departure to close from the current time.
    public static List<TravelAlert> whenADepartsOccurred(Departure departure, MapState<String, AlertMap> requestState, MapState<String, TimeTableMap> timeTableState, String storageKey) throws Exception {
        // Clean time table
        TimeTableMap timeTableMap = timeTableState.get(storageKey);
        if (timeTableMap != null) {
            timeTableMap.remove(departure.getTravelId());
            timeTableState.put(storageKey, timeTableMap);
        }

        // Clean customer requests
        var travelAlerts = getTravelAlertsFromDeparture(requestState, departure);
        requestState.remove(departure.getTravelId());

        return travelAlerts;
    }

    private static List<TravelAlert> getTravelAlertsFromDeparture(MapState<String, AlertMap> requestState, Departure departure) throws Exception {
        AlertMap alertMap = requestState.get(getRequestStateKey(departure.getTravelId()));

        if (alertMap != null) {
            return alertMap.values().stream()
                    // Ugly, alerts are not immutable...
                    .peek(e -> {
                        e.setReason(TravelAlert.ReasonCodes.DEPARTED);
                        e.setUpdateId(departure.getId());
                    })
                    .collect(Collectors.toList());
        } else {
            return List.of();
        }
    }

    public static List<TravelAlert> whenACustomerRequestArose(CustomerTravelRequest request,
                                                              MapState<String, AlertMap> requestState,
                                                              MapState<String, TimeTableMap> timeTableState,
                                                              MapState<String, RequestList> earlyRequestState,
                                                              String storageKey) throws Exception {


        var result = new ArrayList<TravelAlert>();
        // Lookup for matching timetable entry using partition key 'prefix scan' : WE NEED A LIST MODEL
        var timeTableMap = getOrCreateStatestoreEntry(timeTableState, storageKey, TimeTableMap::new);

        // Apply business rules
        var newTravelAlert = TravelAlert.fromRequest(request);
        var optimalTravel = timeTableMap.getOptimalTravel();

        if (optimalTravel != null) {
            updateAlert(newTravelAlert, optimalTravel, null);

            var targetAlertMap = getOrCreateStatestoreEntry(requestState, getRequestStateKey(optimalTravel.getTravelId()), AlertMap::new);

            targetAlertMap.put(newTravelAlert.getId(), newTravelAlert);
            // Store the result
            requestState.put(getRequestStateKey(optimalTravel.getTravelId()), targetAlertMap);

            // Forward the result
            result.add(newTravelAlert);
        } else {
            var requestList = getOrCreateStatestoreEntry(earlyRequestState, storageKey, RequestList::new);

            requestList.add(request);
            // Store the result
            earlyRequestState.put(storageKey, requestList);
        }

        return result;
    }

    private static List<TravelAlert> handleImpactedActiveTravels(
            MapState<String, AlertMap> requestState,
            MapState<String, TimeTableMap> timeTableState,
            String storageKey,
            TimeTableEntry timeUpdate,
            TimeTableMap timeTableMap) throws Exception {

        var result = new ArrayList<TravelAlert>();
        // Lookup for impacted travels
        var currentAlertMap = getOrCreateStatestoreEntry(requestState, getRequestStateKey(timeUpdate.getTravelId()), AlertMap::new);

        List<String> idToRemove = new ArrayList<>();

        for (TravelAlert travelAlert : currentAlertMap.values()) {
            // Apply business rules
            var newOptimal = timeTableMap.getOptimalTravel();

            // Update Request Store
            updateAlert(travelAlert, newOptimal, timeUpdate.getUpdateId());

            // Remove from the old alert map (old optimal)
            idToRemove.add(travelAlert.getId());
            // Update the new alert Map (new optimal)
            var newAlertMap = getOrCreateStatestoreEntry(requestState, getRequestStateKey(newOptimal.getTravelId()), AlertMap::new);

            newAlertMap.put(travelAlert.getId(), travelAlert);

            // Update Stores
            requestState.put(getRequestStateKey(newOptimal.getTravelId()), newAlertMap);

            // Collect
            result.add(travelAlert); // the new alert
        }

        idToRemove.forEach(currentAlertMap::remove);
        requestState.put(getRequestStateKey(timeUpdate.getTravelId()), currentAlertMap);

        timeTableState.put(storageKey, timeTableMap);

        return result;
    }

    private static List<TravelAlert> handleEarlyRequests(MapState<String, AlertMap> requestState, MapState<String, RequestList> earlyRequestState, String storageKey, TimeTableEntry timeUpdate) throws Exception {
        // Check for early requests
        var requestList = getOrCreateStatestoreEntry(earlyRequestState, storageKey, RequestList::new);
        var result = new ArrayList<TravelAlert>();

        List<CustomerTravelRequest> elementToRemove = new ArrayList<>();

        for (CustomerTravelRequest request : requestList) {
            // Update Request Store
            var newAlert = updateAlert(TravelAlert.fromRequest(request), timeUpdate, timeUpdate.getUpdateId());
            newAlert.setUpdateId(timeUpdate.getUpdateId());

            // Remove from the early list
            elementToRemove.add(request);
            // Update the new alert Map (new optimal)
            var newAlertMap = getOrCreateStatestoreEntry(requestState, getRequestStateKey(timeUpdate.getTravelId()), AlertMap::new);

            newAlertMap.put(newAlert.getId(), newAlert);

            // Update Stores
            requestState.put(getRequestStateKey(timeUpdate.getTravelId()), newAlertMap);

            // Collect
            result.add(newAlert); // the new alert
        }

        requestList.removeAll(elementToRemove);

        // Store the result
        earlyRequestState.put(storageKey, requestList);

        return result;
    }
}
