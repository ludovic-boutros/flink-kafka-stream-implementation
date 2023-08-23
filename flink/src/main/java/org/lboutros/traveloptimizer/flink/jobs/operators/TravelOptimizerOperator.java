package org.lboutros.traveloptimizer.flink.jobs.operators;

import lombok.Getter;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.*;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.AlertMap;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.RequestList;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.TimeTableMap;
import org.lboutros.traveloptimizer.flink.jobs.operators.inputs.DepartureInput;
import org.lboutros.traveloptimizer.flink.jobs.operators.inputs.PlaneUpdateInput;
import org.lboutros.traveloptimizer.flink.jobs.operators.inputs.RequestInput;
import org.lboutros.traveloptimizer.flink.jobs.operators.inputs.TrainUpdateInput;
import org.lboutros.traveloptimizer.model.TravelAlert;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings("unchecked")
@Getter
public class TravelOptimizerOperator extends AbstractStreamOperatorV2<TravelAlert>
        implements MultipleInputStreamOperator<TravelAlert> {


    // Key : OptimalTravelID aka Current system state
    private MapState<String, AlertMap> requestState;

    // Key : Departure#Arrival aka Desired Connection
    private MapState<String, RequestList> earlyRequestState;

    // Key : Departure#Arrival aka offered connection
    private MapState<String, TimeTableMap> timeTableState;

    public TravelOptimizerOperator(StreamOperatorParameters<TravelAlert> parameters) {
        super(parameters, 3);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        requestState =
                context.getKeyedStateStore().getMapState(
                        new MapStateDescriptor<>(
                                "Customer Request Status",
                                String.class,
                                AlertMap.class
                        ));
        earlyRequestState =
                context.getKeyedStateStore().getMapState(
                        new MapStateDescriptor<>(
                                "Early Requests",
                                String.class,
                                RequestList.class
                        ));

        timeTableState =
                context.getKeyedStateStore().getMapState(
                        new MapStateDescriptor<>(
                                "TimeTable Statestore",
                                String.class,
                                TimeTableMap.class
                        ));
    }

    @Override
    public List<Input> getInputs() {
        return Arrays.asList(
                new PlaneUpdateInput(this, InputIndexes.PLANE_UPDATE),
                new TrainUpdateInput(this, InputIndexes.TRAIN_UPDATE),
                new RequestInput(this, InputIndexes.REQUEST),
                new DepartureInput(this, InputIndexes.DEPARTURE));
    }

    public interface InputIndexes {
        int PLANE_UPDATE = 1;
        int TRAIN_UPDATE = 2;
        int REQUEST = 3;
        int DEPARTURE = 4;
    }

    public static class TravelOptimizerOperatorFactory
            extends AbstractStreamOperatorFactory<TravelAlert> {
        @Override
        public <T extends StreamOperator<TravelAlert>> T createStreamOperator(
                StreamOperatorParameters<TravelAlert> parameters) {
            return (T) new TravelOptimizerOperator(parameters);
        }

        @Override
        public Class<? extends StreamOperator<TravelAlert>> getStreamOperatorClass(
                ClassLoader classLoader) {
            return TravelOptimizerOperator.class;
        }
    }
}


