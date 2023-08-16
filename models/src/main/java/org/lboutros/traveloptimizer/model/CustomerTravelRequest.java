package org.lboutros.traveloptimizer.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor
public class CustomerTravelRequest {
    private String id;
    private String departureLocation;
    private String arrivalLocation;
}

