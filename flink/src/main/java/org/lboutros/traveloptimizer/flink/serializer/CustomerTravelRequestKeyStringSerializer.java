package org.lboutros.traveloptimizer.flink.serializer;

import org.lboutros.traveloptimizer.model.CustomerTravelRequest;

public class CustomerTravelRequestKeyStringSerializer extends FieldExtractorKeyStringSerializer<CustomerTravelRequest> {

    public String extractKey(CustomerTravelRequest data) {
        return data.getId();
    }

}
