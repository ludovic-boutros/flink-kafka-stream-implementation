package org.lboutros.traveloptimizer.flink.processfunctions;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.lboutros.traveloptimizer.flink.jobs.UnionEnvelope;
import org.lboutros.traveloptimizer.model.TravelAlert;

public class OptimizerFunction extends KeyedProcessFunction<String, UnionEnvelope, TravelAlert> {

    @Override
    public void processElement(UnionEnvelope value, KeyedProcessFunction<String, UnionEnvelope, TravelAlert>.Context ctx, Collector<TravelAlert> out) throws Exception {

    }
}
