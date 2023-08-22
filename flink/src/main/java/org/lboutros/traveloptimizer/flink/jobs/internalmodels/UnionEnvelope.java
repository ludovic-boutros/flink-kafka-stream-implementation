package org.lboutros.traveloptimizer.flink.jobs.internalmodels;

import lombok.*;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.Departure;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;

@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor
@ToString
public class UnionEnvelope {
    private String partitionKey;
    private CustomerTravelRequest customerTravelRequest;
    private PlaneTimeTableUpdate planeTimeTableUpdate;
    private TrainTimeTableUpdate trainTimeTableUpdate;
    private Departure departure;

    public static UnionEnvelope fromCustomerTravelRequest(CustomerTravelRequest customerTravelRequest) {
        UnionEnvelope envelope = new UnionEnvelope();
        envelope.setCustomerTravelRequest(customerTravelRequest);
        envelope.setPartitionKey(customerTravelRequest.getDepartureLocation() + '-' + customerTravelRequest.getArrivalLocation());

        return envelope;
    }

    public static UnionEnvelope fromPlaneTimeTableUpdate(PlaneTimeTableUpdate planeTimeTableUpdate) {
        UnionEnvelope envelope = new UnionEnvelope();
        envelope.setPlaneTimeTableUpdate(planeTimeTableUpdate);
        envelope.setPartitionKey(planeTimeTableUpdate.getDepartureLocation() + '-' + planeTimeTableUpdate.getArrivalLocation());

        return envelope;
    }

    public static UnionEnvelope fromTrainTimeTableUpdate(TrainTimeTableUpdate trainTimeTableUpdate) {
        UnionEnvelope envelope = new UnionEnvelope();
        envelope.setTrainTimeTableUpdate(trainTimeTableUpdate);
        envelope.setPartitionKey(trainTimeTableUpdate.getDepartureLocation() + '-' + trainTimeTableUpdate.getArrivalLocation());

        return envelope;
    }

    public static UnionEnvelope fromDeparture(Departure departure) {
        UnionEnvelope envelope = new UnionEnvelope();
        envelope.setDeparture(departure);
        envelope.setPartitionKey(departure.getDepartureLocation() + '-' + departure.getArrivalLocation());

        return envelope;
    }

    public boolean isCustomerTravelRequest() {
        return customerTravelRequest != null;
    }

    public boolean isDeparture() {
        return departure != null;
    }

}
