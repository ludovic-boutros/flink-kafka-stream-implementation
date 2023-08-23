package org.lboutros.traveloptimizer.kstreams.processors;

import org.apache.kafka.streams.processor.ConnectedStoreProvider;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.lboutros.traveloptimizer.kstreams.configuration.Constants;
import org.lboutros.traveloptimizer.kstreams.models.TimeTableUpdate;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.TravelAlert;

public class TimeTableUpdateProcessor extends ContextualProcessor<String, TimeTableUpdate, String, TravelAlert> implements ConnectedStoreProvider {

    private KeyValueStore<String, TimeTableUpdate> availableConnections;
    private KeyValueStore<String, CustomerTravelRequest> activeRequests;
    private KeyValueStore<String, CustomerTravelRequest> earlyRequests;

    @Override
    public void init(ProcessorContext<String, TravelAlert> context) {
        super.init(context);
        availableConnections = context.getStateStore(Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);
        activeRequests = context.getStateStore(Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);
        earlyRequests = context.getStateStore(Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);
    }

    @Override
    public void process(Record<String, TimeTableUpdate> record) {

    }
}
