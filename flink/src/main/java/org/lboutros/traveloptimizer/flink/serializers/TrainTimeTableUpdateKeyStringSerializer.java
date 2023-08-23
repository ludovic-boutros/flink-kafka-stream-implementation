package org.lboutros.traveloptimizer.flink.serializers;

import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;

public class TrainTimeTableUpdateKeyStringSerializer extends FieldExtractorKeyStringSerializer<TrainTimeTableUpdate> {

    public String extractKey(TrainTimeTableUpdate data) {
        return data.getTravelId();
    }
}
