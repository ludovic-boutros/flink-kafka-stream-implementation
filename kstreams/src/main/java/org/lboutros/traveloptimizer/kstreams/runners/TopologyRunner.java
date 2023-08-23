package org.lboutros.traveloptimizer.kstreams.runner;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Builder
public class TopologyRunner {
    private final Topology topology;
    private final Properties streamsConfiguration;

    public void run() {
        final CountDownLatch latch = new CountDownLatch(1);

        log.info(topology.describe().toString());
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfiguration)) {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
