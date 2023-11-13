package org.lboutros.traveloptimizer.model;

import lombok.*;

@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor
@ToString
public class CustomerTravelRequest {
    private String id;
    private String departureLocation;
    private String arrivalLocation;

    private String hugeDummyData;
}

