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
public class TravelAlert {

    private String id;
    private String departureLocation;
    private String arrivalLocation;
    private ZonedDateTime departureTime;
    private ZonedDateTime arrivalTime;
    private String reason;

    public static TravelAlert fromRequest(CustomerTravelRequest request) {
        var travelAlert = new TravelAlert();

        travelAlert.setId(request.getId());
        travelAlert.setDepartureLocation(request.getDepartureLocation());
        travelAlert.setArrivalLocation(request.getArrivalLocation());

        return travelAlert;
    }
}
