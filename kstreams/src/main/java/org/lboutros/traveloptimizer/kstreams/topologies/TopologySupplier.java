package org.lboutros.traveloptimizer.kstreams.topologies;

import org.apache.kafka.streams.Topology;

import java.util.function.Supplier;

public interface TopologySupplier extends Supplier<Topology> {
}
