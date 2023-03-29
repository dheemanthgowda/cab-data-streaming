package com.learning.flink.datastream.transformations;

import com.learning.flink.datastream.datamodels.Cab;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CalculateAveragePassengers extends AbstractCalculateAverage {
    @Override
    protected Double getValue(Cab value) {
        return Double.valueOf(value.getPassengerCount());
    }
}


