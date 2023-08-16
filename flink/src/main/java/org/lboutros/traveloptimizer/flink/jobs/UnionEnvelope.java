package org.lboutros.traveloptimizer.flink.jobs;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;

@EqualsAndHashCode
@Getter
@Setter
@NoArgsConstructor
public class UnionEnvelope {
    private String partitionKey;
    private CustomerTravelRequest customerTravelRequest;
    private PlaneTimeTableUpdate planeTimeTableUpdate;
    private TrainTimeTableUpdate trainTimeTableUpdate;

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

}
