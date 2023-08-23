package org.lboutros.traveloptimizer.flink.serializers;

import org.lboutros.traveloptimizer.model.CustomerTravelRequest;

public class CustomerTravelRequestKeyStringSerializer extends FieldExtractorKeyStringSerializer<CustomerTravelRequest> {

    public String extractKey(CustomerTravelRequest data) {
        return data.getId();
    }

}
