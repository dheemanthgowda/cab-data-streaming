package com.learning.flink.datastream.transformations;

import com.learning.flink.datastream.datamodels.Cab;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessWindowFunction;

public class WindowAverageAggregator
        implements AggregateFunction<Cab, Tuple2<Long, Long>, Double> {

    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return new Tuple2<>(0L, 0L);
    }

    @Override
    public Tuple2<Long, Long> add(Cab value, Tuple2<Long, Long> accumulator) {
        System.out.println("Adding-- "+value.toString());
        return new Tuple2<>(accumulator.f0 + Long.valueOf(value.getPassengerCount()), accumulator.f1 + 1L);
    }

    @Override
    public Double getResult(Tuple2<Long, Long> accumulator) {
        return (double) accumulator.f0/accumulator.f1;
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f0 + b.f1);
    }
}
