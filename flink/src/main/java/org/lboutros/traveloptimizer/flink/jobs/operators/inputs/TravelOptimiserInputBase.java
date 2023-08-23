package org.lboutros.traveloptimizer.flink.jobs.operators.inputs;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.AlertMap;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.RequestList;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.TimeTableMap;
import org.lboutros.traveloptimizer.flink.jobs.operators.TravelOptimizerOperator;
import org.lboutros.traveloptimizer.model.TravelAlert;

public abstract class TravelOptimiserInputBase<IN> extends AbstractInput<IN, TravelAlert> {
    private TravelOptimizerOperator owner;

    public TravelOptimiserInputBase(AbstractStreamOperatorV2<TravelAlert> owner, int inputId) {
        super(owner, inputId);
        this.owner = (TravelOptimizerOperator) owner;
    }

    protected MapState<String, AlertMap> getRequestState() {
        return owner.getRequestState();
    }

    protected MapState<String, RequestList> getEarlyRequestState() {
        return owner.getEarlyRequestState();
    }

    protected MapState<String, TimeTableMap> getTimeTableState() {
        return owner.getTimeTableState();
    }
}
