package org.lboutros.traveloptimizer.flink.jobs.internalmodels;

import lombok.*;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;

import java.time.ZonedDateTime;

@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor
@ToString
public class TimeTableEntry implements Comparable<TimeTableEntry> {
    private String id;
    private String customerTravelRequest;
    private TRAVEL_TYPE travelType;
    private ZonedDateTime arrivalTime;
    private ZonedDateTime departureTime;

    public static TimeTableEntry fromPlaneTimeTableUpdate(PlaneTimeTableUpdate planeTimeTableUpdate) {
        TimeTableEntry entry = new TimeTableEntry();
        entry.setId(planeTimeTableUpdate.getId());
        entry.setArrivalTime(planeTimeTableUpdate.getArrivalTime());
        entry.setDepartureTime(planeTimeTableUpdate.getDepartureTime());
        entry.setTravelType(TRAVEL_TYPE.PLANE);

        return entry;
    }


    public static TimeTableEntry fromUpdate(UnionEnvelope unionEnvelope) {
        if (unionEnvelope.getTrainTimeTableUpdate() != null) {
            return TimeTableEntry.fromTrainTimeTableUpdate(unionEnvelope.getTrainTimeTableUpdate());
        }

        return TimeTableEntry.fromPlaneTimeTableUpdate(unionEnvelope.getPlaneTimeTableUpdate());

    }

    public static TimeTableEntry fromTrainTimeTableUpdate(TrainTimeTableUpdate trainTimeTableUpdate) {
        TimeTableEntry entry = new TimeTableEntry();
        entry.setId(trainTimeTableUpdate.getId());
        entry.setArrivalTime(trainTimeTableUpdate.getArrivalTime());
        entry.setDepartureTime(trainTimeTableUpdate.getDepartureTime());
        entry.setTravelType(TRAVEL_TYPE.TRAIN);

        return entry;
    }

    @Override
    public int compareTo(TimeTableEntry o) {
        return this.getId().compareTo(o.getId());
    }

    public static enum TRAVEL_TYPE {
        TRAIN,
        PLANE
    }
}
