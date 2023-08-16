package org.lboutros.traveloptimizer.flink.serializer;

import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;

public class PlaneTimeTableUpdateKeyStringSerializer extends FieldExtractorKeyStringSerializer<PlaneTimeTableUpdate> {

    public String extractKey(PlaneTimeTableUpdate data) {
        return data.getId();
    }
}
