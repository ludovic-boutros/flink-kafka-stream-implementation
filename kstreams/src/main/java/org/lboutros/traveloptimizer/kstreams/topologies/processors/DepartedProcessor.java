package org.lboutros.traveloptimizer.kstreams.topologies.processors;

import org.apache.kafka.streams.processor.ConnectedStoreProvider;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.lboutros.traveloptimizer.kstreams.configuration.Constants;
import org.lboutros.traveloptimizer.kstreams.topologies.models.TimeTableEntry;
import org.lboutros.traveloptimizer.model.Departure;
import org.lboutros.traveloptimizer.model.TravelAlert;

import static org.lboutros.traveloptimizer.kstreams.topologies.businessrules.EventManagement.whenADepartsOccurred;

public class DepartedProcessor extends ContextualProcessor<String, Departure, String, TravelAlert> implements ConnectedStoreProvider {

    private KeyValueStore<String, TimeTableEntry> availableConnectionStateStore;
    private KeyValueStore<String, TravelAlert> lastRequestAlertStateStore;

    @Override
    public void init(ProcessorContext<String, TravelAlert> context) {
        super.init(context);
        availableConnectionStateStore = context.getStateStore(Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);
        lastRequestAlertStateStore = context.getStateStore(Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE);
    }

    @Override
    public void process(Record<String, Departure> record) {
        whenADepartsOccurred(record.value(), lastRequestAlertStateStore, availableConnectionStateStore)
                .forEach(travelAlert -> context()
                        .forward(new Record<>(travelAlert.getId(), travelAlert, record.timestamp(), record.headers())));
    }
}
