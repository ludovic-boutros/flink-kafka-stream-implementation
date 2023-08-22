package org.lboutros.traveloptimizer.flink.jobs.processfunctions;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.*;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.Departure;
import org.lboutros.traveloptimizer.model.TravelAlert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.lboutros.traveloptimizer.flink.jobs.internalmodels.Utils.getRequestStateKey;


public class OptimizerFunction extends KeyedProcessFunction<String, UnionEnvelope, TravelAlert> {

    // Key : OptimalTravelID aka Current system state
    private MapStateDescriptor<String, AlertMap> requestStateDescriptor;

    // Key : Departure#Arrival aka Desired Connection
    private MapStateDescriptor<String, RequestList> earlyRequestDescriptor;

    // Key : Departure#Arrival aka offered connection
    private MapStateDescriptor<String, TimeTableMap> timeTableStateDescriptor;


    @Override
    public void open(Configuration parameters) {

        requestStateDescriptor = new MapStateDescriptor<>(
                "Customer Request Status",
                String.class,
                AlertMap.class
        );

        earlyRequestDescriptor = new MapStateDescriptor<>(
                "Early Requests",
                String.class,
                RequestList.class
        );

        timeTableStateDescriptor = new MapStateDescriptor<>(
                "TimeTable Statestore",
                String.class,
                TimeTableMap.class
        );
    }

    @Override
    public void processElement(UnionEnvelope value, KeyedProcessFunction<String, UnionEnvelope, TravelAlert>.Context ctx, Collector<TravelAlert> out) throws Exception {
        MapState<String, AlertMap> requestState = getRuntimeContext().getMapState(requestStateDescriptor);
        MapState<String, TimeTableMap> timeTableState = getRuntimeContext().getMapState(timeTableStateDescriptor);
        MapState<String, RequestList> earlyRequestState = getRuntimeContext().getMapState(earlyRequestDescriptor);

        var storageKey = value.getPartitionKey();

        if (value.isCustomerTravelRequest()) {
            processCustomerTravelRequest(value, out, requestState, timeTableState, earlyRequestState, storageKey);
        } else if (value.isDeparture()) {
            processDeparture(value, out, requestState, timeTableState, storageKey);
        } else {
            processTimeTableUpdate(value, out, requestState, timeTableState, earlyRequestState, storageKey);
        }
    }

    // We don't manage potential timetable state zombie events due to the race condition between departure events and time table update events.
    // We just filter out updates with a departure to close from the current time.
    private void processDeparture(UnionEnvelope value, Collector<TravelAlert> out, MapState<String, AlertMap> requestState, MapState<String, TimeTableMap> timeTableState, String storageKey) throws Exception {
        var departure = value.getDeparture();

        // Clean time table
        TimeTableMap timeTableMap = timeTableState.get(storageKey);
        if (timeTableMap != null) {
            timeTableMap.remove(departure.getTravelId());
            timeTableState.put(storageKey, timeTableMap);
        }

        // Clean customer requests
        Collection<TravelAlert> travelAlerts = getTravelAlertsFromDeparture(requestState, departure);
        requestState.remove(departure.getTravelId());

        travelAlerts.forEach(out::collect);
    }

    private Collection<TravelAlert> getTravelAlertsFromDeparture(MapState<String, AlertMap> requestState, Departure departure) throws Exception {
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

    private void processCustomerTravelRequest(UnionEnvelope value,
                                              Collector<TravelAlert> out,
                                              MapState<String, AlertMap> requestState,
                                              MapState<String, TimeTableMap> timeTableState,
                                              MapState<String, RequestList> earlyRequestState,
                                              String storageKey) throws Exception {
        var request = value.getCustomerTravelRequest();

        // Lookup for matching timetable entry using partition key 'prefix scan' : WE NEED A LIST MODEL
        var timeTableMap = getOrCreateStatestoreEntry(timeTableState, storageKey, TimeTableMap::new);

        // Apply business rules
        var newTravelAlert = TravelAlert.fromRequest(request);
        var optimalTravel = getOptimalTravel(timeTableMap);

        if (optimalTravel != null) {
            updateAlert(newTravelAlert, optimalTravel, null);

            var targetAlertMap = getOrCreateStatestoreEntry(requestState, getRequestStateKey(optimalTravel.getTravelId()), AlertMap::new);

            targetAlertMap.put(newTravelAlert.getId(), newTravelAlert);
            // Store the result
            requestState.put(getRequestStateKey(optimalTravel.getTravelId()), targetAlertMap);

            // Forward the result
            out.collect(newTravelAlert);
        } else {
            var requestList = getOrCreateStatestoreEntry(earlyRequestState, storageKey, RequestList::new);

            requestList.add(request);
            // Store the result
            earlyRequestState.put(storageKey, requestList);
        }
    }

    private void processTimeTableUpdate(UnionEnvelope value, Collector<TravelAlert> out, MapState<String, AlertMap> requestState, MapState<String, TimeTableMap> timeTableState, MapState<String, RequestList> earlyRequestState, String storageKey) throws Exception {
        TimeTableEntry timeUpdate = TimeTableEntry.fromUpdate(value);
        var timeTableMap = getOrCreateStatestoreEntry(timeTableState, storageKey, TimeTableMap::new);

        // Upsert the new timetable entry
        timeTableMap.put(timeUpdate.getTravelId(), timeUpdate);

        handleImpactedActiveTravels(out, requestState, timeTableState, storageKey, timeUpdate, timeTableMap);
        handleEarlyRequests(out, requestState, earlyRequestState, storageKey, timeUpdate);
    }

    private void handleImpactedActiveTravels(Collector<TravelAlert> out, MapState<String, AlertMap> requestState, MapState<String, TimeTableMap> timeTableState, String storageKey, TimeTableEntry timeUpdate, TimeTableMap timeTableMap) throws Exception {
        // Lookup for impacted travels
        var currentAlertMap = getOrCreateStatestoreEntry(requestState, getRequestStateKey(timeUpdate.getTravelId()), AlertMap::new);

        List<String> idToRemove = new ArrayList<>();

        for (TravelAlert travelAlert : currentAlertMap.values()) {
            // Apply business rules
            var newOptimal = getOptimalTravel(timeTableMap);

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
            out.collect(travelAlert); // the new alert
        }

        idToRemove.forEach(currentAlertMap::remove);
        requestState.put(getRequestStateKey(timeUpdate.getTravelId()), currentAlertMap);

        timeTableState.put(storageKey, timeTableMap);
    }

    private void handleEarlyRequests(Collector<TravelAlert> out, MapState<String, AlertMap> requestState, MapState<String, RequestList> earlyRequestState, String storageKey, TimeTableEntry timeUpdate) throws Exception {
        // Check for early requests
        var requestList = getOrCreateStatestoreEntry(earlyRequestState, storageKey, RequestList::new);

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
            out.collect(newAlert); // the new alert
        }

        requestList.removeAll(elementToRemove);

        // Store the result
        earlyRequestState.put(storageKey, requestList);
    }

    private TravelAlert updateAlert(TravelAlert alert, TimeTableEntry newOptimal, String updateId) {

        alert.setArrivalTime(newOptimal.getArrivalTime());
        alert.setDepartureTime(newOptimal.getDepartureTime());
        alert.setLastTravelId(alert.getTravelId());
        alert.setTravelId(newOptimal.getTravelId());
        alert.setTravelType(newOptimal.getTravelType());
        alert.setUpdateId(updateId);

        return alert;
    }


    private TimeTableEntry getOptimalTravel(TimeTableMap availableTravel) {

        if (availableTravel == null) {
            return null;
        }

        var result = availableTravel.values().stream().min(Comparator.comparing(TimeTableEntry::getArrivalTime));

        return result.orElse(null);
    }

    private <T> T getOrCreateStatestoreEntry(MapState<String, T> state, String key, Supplier<T> creator) throws Exception {
        T retValue = state.get(key);
        if (retValue == null) {
            retValue = creator.get();
        }
        return retValue;
    }
}
