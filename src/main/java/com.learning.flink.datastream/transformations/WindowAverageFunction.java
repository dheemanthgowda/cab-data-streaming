package com.learning.flink.datastream.transformations;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowAverageFunction extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<Double,
            Tuple2<String, Double>, String, TimeWindow>.Context context,
                        Iterable<Double> elements, Collector<Tuple2<String, Double>> out) throws Exception {
        System.out.println(context.window().getStart() + "===" + context.window().getEnd());
        out.collect(new Tuple2<>(s, elements.iterator().next()));
    }
}
