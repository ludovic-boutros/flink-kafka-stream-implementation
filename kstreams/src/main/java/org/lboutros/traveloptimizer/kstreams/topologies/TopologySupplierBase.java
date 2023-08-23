package org.lboutros.traveloptimizer.kstreams.topologies;

import lombok.Getter;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;

import static org.lboutros.traveloptimizer.kstreams.configuration.Utils.setApplicationId;

@Getter
public abstract class TopologySupplierBase implements TopologySupplier {
    private final Properties streamsConfiguration = new Properties();
    private final StreamsBuilder streamsBuilder = new StreamsBuilder();

    public TopologySupplierBase(Properties streamsConfiguration, String applicationId) {
        setApplicationId(streamsConfiguration, applicationId);
        this.streamsConfiguration.putAll(streamsConfiguration);
    }
}
