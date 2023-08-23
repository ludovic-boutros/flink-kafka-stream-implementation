package org.lboutros.traveloptimizer.kstreams.models;

import lombok.*;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;

import java.time.ZonedDateTime;

@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor
@ToString
@Builder
@AllArgsConstructor
public class TimeTableUpdate {
    private String updateId;
    private String travelId;
    private String departureLocation;
    private String arrivalLocation;
    private ZonedDateTime departureTime;
    private ZonedDateTime arrivalTime;

    public static TimeTableUpdate fromTrainTimeTableUpdate(TrainTimeTableUpdate trainTimeTableUpdate) {
        return TimeTableUpdate.builder()
                .arrivalLocation(trainTimeTableUpdate.getArrivalLocation())
                .departureLocation(trainTimeTableUpdate.getDepartureLocation())
                .updateId(trainTimeTableUpdate.getUpdateId())
                .travelId(trainTimeTableUpdate.getTravelId())
                .departureTime(trainTimeTableUpdate.getDepartureTime())
                .arrivalTime(trainTimeTableUpdate.getArrivalTime())
                .build();
    }

    public static TimeTableUpdate fromPlaneTimeTableUpdate(PlaneTimeTableUpdate trainTimeTableUpdate) {
        return TimeTableUpdate.builder()
                .arrivalLocation(trainTimeTableUpdate.getArrivalLocation())
                .departureLocation(trainTimeTableUpdate.getDepartureLocation())
                .updateId(trainTimeTableUpdate.getUpdateId())
                .travelId(trainTimeTableUpdate.getTravelId())
                .departureTime(trainTimeTableUpdate.getDepartureTime())
                .arrivalTime(trainTimeTableUpdate.getArrivalTime())
                .build();
    }
}