package com.learning.flink.datastream.transformations;

import com.learning.flink.datastream.datamodels.Cab;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CalculateAverageTrip extends AbstractCalculateAverage {
    @Override
    protected Double getValue(Cab value) {
        return Double.valueOf(value.getPassengerCount());
    }

    @Override
    public void flatMap(Cab value, Collector<Tuple2<String, Double>> out) throws Exception {
        Tuple2<Long, Double> currentSum = sum.value() == null ? Tuple2.of(0L, 0.0) : sum.value();
        currentSum.f0 += 1;
        currentSum.f1 += getValue(value);
        sum.update(currentSum);
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(value.getCabDriverName(), currentSum.f1 / currentSum.f0));
        }
    }
}
