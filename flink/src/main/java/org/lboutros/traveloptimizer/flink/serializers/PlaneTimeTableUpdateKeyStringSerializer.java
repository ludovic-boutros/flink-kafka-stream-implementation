package org.lboutros.traveloptimizer.flink.serializers;

import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;

public class PlaneTimeTableUpdateKeyStringSerializer extends FieldExtractorKeyStringSerializer<PlaneTimeTableUpdate> {

    public String extractKey(PlaneTimeTableUpdate data) {
        return data.getTravelId();
    }
}
