package com.learning.flink.datastream;

import com.learning.flink.datastream.datamodels.Cab;
import com.learning.flink.datastream.transformations.WindowAverageFunction;
import com.learning.flink.datastream.transformations.WindowAverageAggregator;
import javax.annotation.Nullable;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowingPractise {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env =  StreamExecutionEnvironment.createLocalEnvironment(3);

    DataStream<String> source = env.socketTextStream("localhost", 9090);
    DataStream<Cab> stream = source.map(new MapFunction<String, Cab>() {
      @Override
      public Cab map(String value) throws Exception {
        String[] eachLine = value.split(",");
        final Cab input = new Cab(eachLine[0],
                eachLine[1],
                eachLine[2],
                eachLine[3],
                eachLine[4],
                eachLine[5],
                eachLine[6],
                eachLine[7].equals("'null'") ? null : Integer.valueOf(eachLine[7]),
                Long.parseLong(eachLine[8]));
        return input;
      }
    }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Cab>() {
      @Override
      public long extractAscendingTimestamp(Cab element) {
        return element.getTimeStamp();
      }
    });


//    stream.filter((FilterFunction<Cab>) value -> value.getOnGoing().equals("yes"))
//            .keyBy((KeySelector<Cab, String>) Cab::getCabDriverName)
//            .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
//            .aggregate(new WindowAverageAggregator(), new WindowAverageFunction()).print("Processing window");

    stream.filter((FilterFunction<Cab>) value -> value.getOnGoing().equals("yes"))
            .keyBy((KeySelector<Cab, String>) Cab::getCabDriverName)
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new WindowAverageAggregator(), new WindowAverageFunction())
            .print("Event window");
    env.execute("WindowingPractise");
  }
}
