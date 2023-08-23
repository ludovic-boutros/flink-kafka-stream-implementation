package org.lboutros.traveloptimizer.flink.jobs.processfunctions;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.*;
import org.lboutros.traveloptimizer.model.TravelAlert;

import static org.lboutros.traveloptimizer.flink.jobs.businessrules.EventManagement.*;


public class TravelOptimizerFunction extends KeyedProcessFunction<String, UnionEnvelope, TravelAlert> {

    // Key : OptimalTravelID aka Current system state
    private MapStateDescriptor<String, AlertMap> requestStateDescriptor;

    // Key : Departure#Arrival aka Desired Connection
    private MapStateDescriptor<String, RequestList> earlyRequestDescriptor;

    // Key : Departure#Arrival aka offered connection
    private MapStateDescriptor<String, TimeTableMap> timeTableStateDescriptor;


    @Override
    public void open(Configuration parameters) {

        requestStateDescriptor = new MapStateDescriptor<>(
                "Customer Request Status",
                String.class,
                AlertMap.class
        );

        earlyRequestDescriptor = new MapStateDescriptor<>(
                "Early Requests",
                String.class,
                RequestList.class
        );

        timeTableStateDescriptor = new MapStateDescriptor<>(
                "TimeTable Statestore",
                String.class,
                TimeTableMap.class
        );
    }

    @Override
    public void processElement(UnionEnvelope value, KeyedProcessFunction<String, UnionEnvelope, TravelAlert>.Context ctx, Collector<TravelAlert> out) throws Exception {
        MapState<String, AlertMap> requestState = getRuntimeContext().getMapState(requestStateDescriptor);
        MapState<String, TimeTableMap> timeTableState = getRuntimeContext().getMapState(timeTableStateDescriptor);
        MapState<String, RequestList> earlyRequestState = getRuntimeContext().getMapState(earlyRequestDescriptor);

        var storageKey = value.getPartitionKey();

        if (value.isCustomerTravelRequest()) {
            whenACustomerRequestArose(value.getCustomerTravelRequest(), requestState, timeTableState, earlyRequestState, storageKey)
                    .forEach(out::collect);
        } else if (value.isDeparture()) {
            whenADepartsOccurred(value.getDeparture(), requestState, timeTableState, storageKey)
                    .forEach(out::collect);
        } else {
            whenATimeTableUpdateArose(TimeTableEntry.fromUpdate(value), requestState, timeTableState, earlyRequestState, storageKey)
                    .forEach(out::collect);
            ;
        }
    }


}
