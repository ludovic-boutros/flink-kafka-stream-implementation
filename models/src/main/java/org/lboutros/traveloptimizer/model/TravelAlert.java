package org.lboutros.traveloptimizer.model;

import lombok.*;

import java.time.ZonedDateTime;

@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor
@ToString
public class TravelAlert {

    private String id;
    private String travelId;
    private String lastTravelId;
    private String updateId;
    private TravelType travelType;
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
