package org.lboutros.traveloptimizer.flink.serializer;

import org.lboutros.traveloptimizer.model.Departure;

public class DepartureKeyStringSerializer extends FieldExtractorKeyStringSerializer<Departure> {

    public String extractKey(Departure data) {
        return data.getId();
    }

}
