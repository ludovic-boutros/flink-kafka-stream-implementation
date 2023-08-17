package org.lboutros.traveloptimizer.flink.processfunctions;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.AlertMap;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.TimeTableEntry;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.TimeTableMap;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.UnionEnvelope;
import org.lboutros.traveloptimizer.model.TravelAlert;

import java.util.Comparator;


public class OptimizerFunction extends KeyedProcessFunction<String, UnionEnvelope, TravelAlert> {

    private MapStateDescriptor<String, AlertMap> requestStateDescriptor;
    private MapStateDescriptor<String, TimeTableMap> timeTableStateDescriptor;


    @Override
    public void open(Configuration parameters) {

        requestStateDescriptor = new MapStateDescriptor<>(
                "Customer Request Status",
                String.class,
                AlertMap.class
        );

        timeTableStateDescriptor = new MapStateDescriptor<>(
                "TimeTable Statestore",
                String.class,
                TimeTableMap.class
        );

        //TODO third statestore for orphan requests
    }

    @Override
    public void processElement(UnionEnvelope value, KeyedProcessFunction<String, UnionEnvelope, TravelAlert>.Context ctx, Collector<TravelAlert> out) throws Exception {
        MapState<String, AlertMap> requestState = getRuntimeContext().getMapState(requestStateDescriptor);
        MapState<String, TimeTableMap> timeTableState = getRuntimeContext().getMapState(timeTableStateDescriptor);

        var storageKey = value.getPartitionKey();

        // If it's a request
        if (value.getCustomerTravelRequest() != null) {

            var request = value.getCustomerTravelRequest();

            // Lookup for matching timetable entry using partition key 'prefix scan' : WE NEED A LIST MODEL
            var timeTableMap = timeTableState.get(storageKey);

            if (timeTableMap == null) {
                timeTableMap = new TimeTableMap();
            }

            // Apply business rules

            var newTravelAlert = TravelAlert.fromRequest(request);
            var optimalTravel = getOptimalTravel(timeTableMap);

            if (optimalTravel != null) {

                updateAlert(newTravelAlert, optimalTravel);

                var targetAlertMap = requestState.get(optimalTravel.getId());

                if (targetAlertMap == null) {
                    targetAlertMap = new AlertMap();
                }
                targetAlertMap.put(newTravelAlert.getId(), newTravelAlert);
                // Store the result
                requestState.put(optimalTravel.getId(), targetAlertMap);

                // Forward the result
                out.collect(newTravelAlert);
            }

        } else {
            TimeTableEntry timeUpdate = TimeTableEntry.fromUpdate(value);
            var timeTableMap = timeTableState.get(storageKey);

            if (timeTableMap == null) {
                timeTableMap = new TimeTableMap();
            }

            // Upsert the new timetable entry
            timeTableMap.put(timeUpdate.getId(), timeUpdate);

            var currentAlertMap = requestState.get(timeUpdate.getId());

            if (currentAlertMap == null) {
                currentAlertMap = new AlertMap();
            }


            for (TravelAlert travelAlert : currentAlertMap.values()) {
                // Apply business rules

                var newOptimal = getOptimalTravel(timeTableMap);

                // Update Request Store
                if (!newOptimal.getId().equals(timeUpdate.getId())) {

                    updateAlert(travelAlert, newOptimal);

                    // Remove from the old alert map (old optimal)
                    currentAlertMap.remove(travelAlert.getId());
                    // Update the new alert Map (new optimal)
                    var newAlertMap = requestState.get(newOptimal.getId());

                    if (newAlertMap == null) {
                        newAlertMap = new AlertMap();
                    }

                    newAlertMap.put(travelAlert.getId(), travelAlert);

                    // Update Stores
                    requestState.put(newOptimal.getId(), newAlertMap);
                    requestState.put(travelAlert.getId(), currentAlertMap);

                    // Collect
                    out.collect(travelAlert); // the new alert
                }

            }

            timeTableState.put(storageKey, timeTableMap);

        }
    }

    private TravelAlert updateAlert(TravelAlert alert, TimeTableEntry newOptimal) {

        alert.setArrivalTime(newOptimal.getArrivalTime());
        alert.setDepartureTime(newOptimal.getDepartureTime());

        return alert;
    }


    private TimeTableEntry getOptimalTravel(TimeTableMap availableTravel) {

        if (availableTravel == null) {
            return null;
        }

        var result = availableTravel.values().stream().min(Comparator.comparing(TimeTableEntry::getArrivalTime));
        if (result.isEmpty()) {
            return null;
        }

        return result.get();
    }

}
