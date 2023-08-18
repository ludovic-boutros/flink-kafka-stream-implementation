package org.lboutros.traveloptimizer.flink.serializer;

import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;

public class TrainTimeTableUpdateKeyStringSerializer extends FieldExtractorKeyStringSerializer<TrainTimeTableUpdate> {

    public String extractKey(TrainTimeTableUpdate data) {
        return data.getTravelId();
    }
}
