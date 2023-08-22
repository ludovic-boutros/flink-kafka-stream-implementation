package org.lboutros.traveloptimizer.flink.jobs.internalmodels;

import lombok.*;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TravelType;

import java.time.ZonedDateTime;

@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor
@ToString
public class TimeTableEntry implements Comparable<TimeTableEntry> {
    private String updateId;
    private String travelId;
    private TravelType travelType;
    private ZonedDateTime arrivalTime;
    private ZonedDateTime departureTime;

    public static TimeTableEntry fromPlaneTimeTableUpdate(PlaneTimeTableUpdate planeTimeTableUpdate) {
        TimeTableEntry entry = new TimeTableEntry();
        entry.setUpdateId(planeTimeTableUpdate.getUpdateId());
        entry.setTravelId(planeTimeTableUpdate.getTravelId());
        entry.setArrivalTime(planeTimeTableUpdate.getArrivalTime());
        entry.setDepartureTime(planeTimeTableUpdate.getDepartureTime());
        entry.setTravelType(TravelType.PLANE);

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
        entry.setTravelId(trainTimeTableUpdate.getTravelId());
        entry.setUpdateId(trainTimeTableUpdate.getUpdateId());
        entry.setArrivalTime(trainTimeTableUpdate.getArrivalTime());
        entry.setDepartureTime(trainTimeTableUpdate.getDepartureTime());
        entry.setTravelType(TravelType.TRAIN);

        return entry;
    }

    @Override
    public int compareTo(TimeTableEntry o) {
        return this.getTravelId().compareTo(o.getTravelId());
    }

}
