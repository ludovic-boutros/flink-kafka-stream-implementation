package org.lboutros.traveloptimizer.model;

import lombok.*;

import java.time.ZonedDateTime;

@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Departure {
    private String id;
    private String travelId;
    private String departureLocation;
    private String arrivalLocation;
    private ZonedDateTime effectiveDepartureTime;
    private ZonedDateTime expectedArrivalTime;
}
