package org.lboutros.traveloptimizer.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.ZonedDateTime;

@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor
public class TrainTimeTableUpdate {
    private String id;
    private String departureLocation;
    private String arrivalLocation;
    private ZonedDateTime departureTime;
    private ZonedDateTime arrivalTime;
}
