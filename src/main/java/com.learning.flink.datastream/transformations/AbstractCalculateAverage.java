package com.learning.flink.datastream.transformations;

import com.learning.flink.datastream.datamodels.Cab;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public abstract class AbstractCalculateAverage extends RichFlatMapFunction<Cab, Tuple2<String, Double>> {

     protected transient ValueState<Tuple2<Long, Double>> sum;


    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Double>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {}));
        sum = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Cab value, Collector<Tuple2<String, Double>> out) throws Exception {
        Tuple2<Long, Double> currentSum = sum.value() == null ? Tuple2.of(0L, 0.0) : sum.value();
        currentSum.f0 += 1;
        currentSum.f1 += getValue(value);
        sum.update(currentSum);
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(value.getPickupLocation(), currentSum.f1 / currentSum.f0));
        }
    }

    @Override
    public void close() {
        sum.clear();
    }

    protected abstract Double getValue(Cab value);
}
