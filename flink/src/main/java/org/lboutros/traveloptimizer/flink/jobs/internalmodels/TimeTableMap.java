package org.lboutros.traveloptimizer.flink.jobs.internalmodels;

import java.util.Comparator;
import java.util.HashMap;

public class TimeTableMap extends HashMap<String, TimeTableEntry> {

    public TimeTableEntry getOptimalTravel() {

        var result = this.values().stream().min(Comparator.comparing(TimeTableEntry::getArrivalTime));

        return result.orElse(null);
    }
}