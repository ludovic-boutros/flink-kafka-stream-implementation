package org.lboutros.traveloptimizer.kstreams.topologies.processors;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.lboutros.traveloptimizer.kstreams.configuration.Constants;
import org.lboutros.traveloptimizer.kstreams.topologies.models.TimeTableEntry;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.TravelAlert;

import static org.lboutros.traveloptimizer.kstreams.topologies.businessrules.EventManagement.whenACustomerRequestArose;

public class CustomerTravelRequestProcessor extends ContextualProcessor<String, CustomerTravelRequest, String, TravelAlert> {

    private KeyValueStore<String, TimeTableEntry> availableConnectionStateStore;
    private KeyValueStore<String, TravelAlert> lastRequestAlertStateStore;
    private KeyValueStore<String, CustomerTravelRequest> earlyRequestStateStore;

    @Override
    public void init(ProcessorContext<String, TravelAlert> context) {
        super.init(context);
        availableConnectionStateStore = context.getStateStore(Constants.StateStores.AVAILABLE_CONNECTIONS_STATE_STORE);
        lastRequestAlertStateStore = context.getStateStore(Constants.StateStores.LAST_REQUEST_ALERT_STATE_STORE);
        earlyRequestStateStore = context.getStateStore(Constants.StateStores.EARLY_REQUESTS_STATE_STORE);
    }

    @Override
    public void process(Record<String, CustomerTravelRequest> record) {
        whenACustomerRequestArose(record.value(), lastRequestAlertStateStore, availableConnectionStateStore, earlyRequestStateStore)
                .forEach(travelAlert -> context()
                        .forward(new Record<>(travelAlert.getId(), travelAlert, record.timestamp(), record.headers())));
    }
}
