package org.lboutros.traveloptimizer.kstreams.processors;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.lboutros.traveloptimizer.kstreams.configuration.Constants;
import org.lboutros.traveloptimizer.kstreams.models.TimeTableUpdate;
import org.lboutros.traveloptimizer.model.TravelAlert;

public class TrainTimeTableUpdateProcessor extends ContextualProcessor<String, TimeTableUpdate, String, TravelAlert> {

    private KeyValueStore<String, TimeTableUpdate> kvStore;

    @Override
    public void init(ProcessorContext<String, TravelAlert> context) {
        super.init(context);
        kvStore = context.getStateStore(Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);
    }

    @Override
    public void process(Record<String, TimeTableUpdate> record) {

    }
}
