package org.lboutros.traveloptimizer.flink.jobs.operators.inputs;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.lboutros.traveloptimizer.flink.jobs.businessrules.EventManagement;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.TimeTableEntry;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.Utils;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TravelAlert;

public class TrainUpdateInput extends TravelOptimiserInputBase<TrainTimeTableUpdate> {
    public TrainUpdateInput(AbstractStreamOperatorV2<TravelAlert> owner, int inputId) {
        super(owner, inputId);
    }

    @Override
    public void processElement(StreamRecord<TrainTimeTableUpdate> element) throws Exception {

        var timeTableUpdate = element.getValue();
        var storageKey = Utils.getPartitionKey(timeTableUpdate.getDepartureLocation(), timeTableUpdate.getArrivalLocation());
        var timeTableState = getTimeTableState();
        var requestState = getRequestState();
        var earlyRequestState = getEarlyRequestState();


        EventManagement.whenATimeTableUpdateArose(TimeTableEntry.fromTrainTimeTableUpdate(timeTableUpdate), requestState, timeTableState, earlyRequestState, storageKey)
                .stream().map(StreamRecord::new)
                .forEach(output::collect);
    }
}
