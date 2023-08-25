package org.lboutros.traveloptimizer.flink.datagen;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.lboutros.traveloptimizer.model.generator.DataGenerator.generateAirportCode;

class DataGeneratorTest {

    @Test
    public void airportGeneration() {
        String departure = generateAirportCode(1);
        assertEquals("ATL", departure);

        String arrival = generateAirportCode(1, departure);
        assertEquals("DFW", arrival);
    }
}