package org.lboutros.traveloptimizer.flink.jobs.operators.inputs;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.lboutros.traveloptimizer.flink.jobs.businessrules.EventManagement;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.Utils;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.TravelAlert;

public class RequestInput extends TravelOptimiserInputBase<CustomerTravelRequest> {
    public RequestInput(AbstractStreamOperatorV2<TravelAlert> owner, int inputId) {
        super(owner, inputId);
    }

    @Override
    public void processElement(StreamRecord<CustomerTravelRequest> element) throws Exception {

        var request = element.getValue();
        var storageKey = Utils.getPartitionKey(request.getDepartureLocation(), request.getArrivalLocation());
        var timeTableState = getTimeTableState();
        var requestState = getRequestState();
        var earlyRequestState = getEarlyRequestState();

        EventManagement.whenACustomerRequestArose(request, requestState, timeTableState, earlyRequestState, storageKey)
                .stream().map(StreamRecord::new)
                .forEach(output::collect);
    }
}
