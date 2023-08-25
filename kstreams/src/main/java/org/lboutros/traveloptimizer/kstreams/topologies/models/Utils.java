package org.lboutros.traveloptimizer.kstreams.topologies.models;

import org.lboutros.traveloptimizer.model.TravelAlert;

public class Utils {
    public static String getStateStoreSearchPrefix(String departure, String arrival) {
        return departure + '#' + arrival;
    }

    public static TravelAlert updateAlert(TravelAlert alert, TimeTableEntry newOptimal, String updateId) {
        alert.setArrivalTime(newOptimal.getArrivalTime());
        alert.setDepartureTime(newOptimal.getDepartureTime());
        alert.setLastTravelId(alert.getTravelId());
        alert.setTravelId(newOptimal.getTravelId());
        alert.setTravelType(newOptimal.getTravelType());
        alert.setUpdateId(updateId);

        return alert;
    }

    public static String getRequestStateKey(String travelId, String requestId) {
        return travelId + '#' + requestId;
    }

    public static String getEarlyRequestStateKey(String departure, String arrival, String requestId) {
        return getStateStoreSearchPrefix(departure, arrival) + '#' + requestId;
    }
}
