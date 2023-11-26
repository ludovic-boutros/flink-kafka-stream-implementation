package org.lboutros.traveloptimizer.kstreams.topologies.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class BasicProcessor implements Processor<String, String, String, String> {
    @Override
    public void init(ProcessorContext<String, String> context) {
        Processor.super.init(context);
    }

    @Override
    public void process(Record<String, String> record) {

    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
