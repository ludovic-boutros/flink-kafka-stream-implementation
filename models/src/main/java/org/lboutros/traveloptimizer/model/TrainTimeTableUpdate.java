package org.lboutros.traveloptimizer.model;

import lombok.*;

import java.time.ZonedDateTime;

@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor
@ToString
public class TrainTimeTableUpdate {
    private String id;
    private String departureLocation;
    private String arrivalLocation;
    private ZonedDateTime departureTime;
    private ZonedDateTime arrivalTime;
}
