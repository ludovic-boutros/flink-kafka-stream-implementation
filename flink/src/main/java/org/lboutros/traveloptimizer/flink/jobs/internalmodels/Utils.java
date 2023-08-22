package org.lboutros.traveloptimizer.flink.jobs.internalmodels;

public class Utils {
    public static String getRequestStateKey(String travelId) {
        return travelId.split("_")[0];
    }
}
