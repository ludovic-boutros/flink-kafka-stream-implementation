package org.lboutros.traveloptimizer.flink.jobs.internalmodels;

import org.apache.flink.api.common.state.MapState;
import org.lboutros.traveloptimizer.model.TravelAlert;

import java.util.function.Supplier;

public class Utils {
    public static String getRequestStateKey(String travelId) {
        return travelId.split("_")[0];
    }


    public static String getPartitionKey(String departure, String arrival) {
        return departure + '-' + arrival;
    }

    public static <T> T getOrCreateStatestoreEntry(MapState<String, T> state, String key, Supplier<T> creator) throws Exception {
        T retValue = state.get(key);
        if (retValue == null) {
            retValue = creator.get();
        }
        return retValue;
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
}
