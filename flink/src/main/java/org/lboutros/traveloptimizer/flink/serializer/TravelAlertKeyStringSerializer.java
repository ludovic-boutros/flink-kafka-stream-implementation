package org.lboutros.traveloptimizer.flink.serializer;

import org.lboutros.traveloptimizer.model.TravelAlert;

public class TravelAlertKeyStringSerializer extends FieldExtractorKeyStringSerializer<TravelAlert> {

    public String extractKey(TravelAlert data) {
        return data.getTravelId();
    }
}
