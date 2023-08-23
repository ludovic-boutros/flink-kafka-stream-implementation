package org.lboutros.traveloptimizer.flink.jobs.operators.inputs;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.lboutros.traveloptimizer.flink.jobs.businessrules.EventManagement;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.Utils;
import org.lboutros.traveloptimizer.model.Departure;
import org.lboutros.traveloptimizer.model.TravelAlert;

public class DepartureInput extends TravelOptimiserInputBase<Departure> {

    public DepartureInput(AbstractStreamOperatorV2<TravelAlert> owner, int inputId) {
        super(owner, inputId);
    }

    @Override
    public void processElement(StreamRecord<Departure> element) throws Exception {

        var departure = element.getValue();
        var storageKey = Utils.getPartitionKey(departure.getDepartureLocation(), departure.getArrivalLocation());
        var timeTableState = getTimeTableState();
        var requestState = getRequestState();

        EventManagement.whenADepartsOccurred(departure, requestState, timeTableState, storageKey)
                .stream().map(StreamRecord::new)
                .forEach(output::collect);
    }
}
