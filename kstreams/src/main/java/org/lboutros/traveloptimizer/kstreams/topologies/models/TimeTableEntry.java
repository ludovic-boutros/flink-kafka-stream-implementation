package org.lboutros.traveloptimizer.kstreams.topologies.models;

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
@Builder
@AllArgsConstructor
public class TimeTableEntry {
    private String updateId;
    private String travelId;
    private String departureLocation;
    private String arrivalLocation;
    private ZonedDateTime departureTime;
    private ZonedDateTime arrivalTime;
    private TravelType travelType;

    public static TimeTableEntry fromTrainTimeTableUpdate(TrainTimeTableUpdate trainTimeTableUpdate) {
        return TimeTableEntry.builder()
                .arrivalLocation(trainTimeTableUpdate.getArrivalLocation())
                .departureLocation(trainTimeTableUpdate.getDepartureLocation())
                .updateId(trainTimeTableUpdate.getUpdateId())
                .travelId(trainTimeTableUpdate.getTravelId())
                .departureTime(trainTimeTableUpdate.getDepartureTime())
                .arrivalTime(trainTimeTableUpdate.getArrivalTime())
                .travelType(TravelType.TRAIN)
                .build();
    }

    public static TimeTableEntry fromPlaneTimeTableUpdate(PlaneTimeTableUpdate planeTimeTableUpdate) {
        return TimeTableEntry.builder()
                .arrivalLocation(planeTimeTableUpdate.getArrivalLocation())
                .departureLocation(planeTimeTableUpdate.getDepartureLocation())
                .updateId(planeTimeTableUpdate.getUpdateId())
                .travelId(planeTimeTableUpdate.getTravelId())
                .departureTime(planeTimeTableUpdate.getDepartureTime())
                .arrivalTime(planeTimeTableUpdate.getArrivalTime())
                .travelType(TravelType.PLANE)
                .build();
    }
}