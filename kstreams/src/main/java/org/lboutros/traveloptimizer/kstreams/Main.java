package org.lboutros.traveloptimizer.kstreams;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.kafka.streams.Topology;
import org.lboutros.traveloptimizer.kstreams.runners.TopologyRunner;
import org.lboutros.traveloptimizer.kstreams.topologies.TravelOptimizerTopologySupplier;

import java.util.Properties;

import static org.lboutros.traveloptimizer.kstreams.configuration.Utils.readConfiguration;

@Slf4j
public class Main {

    // Main class by default
    public static void main(final String[] args) throws ConfigurationException {
        CustomerRegionKStreamKStreamJoiner.main(args);
    }

    public static class CustomerRegionKStreamKStreamJoiner {
        public static void main(String[] args) throws ConfigurationException {
            final Properties streamsConfiguration = readConfiguration("application.properties");

            log.info("Configuration properties: {}", streamsConfiguration);

            Topology topology = TravelOptimizerTopologySupplier.builder()
                    .streamsConfiguration(streamsConfiguration)
                    .build()
                    .get();

            TopologyRunner.builder()
                    .streamsConfiguration(streamsConfiguration)
                    .topology(topology)
                    .build()
                    .run();
        }
    }

}
