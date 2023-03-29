package com.learning.flink.datastream;

import com.learning.flink.datastream.datamodels.Cab;
import com.learning.flink.datastream.datasource.CabDataSource;
import com.learning.flink.datastream.transformations.CalculateAveragePassengers;
import com.learning.flink.datastream.transformations.CalculateAverageTrip;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CabDataStreamingApp {
  private static final Logger LOG = LoggerFactory.getLogger(CabDataStreamingApp.class);

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(3);

    DataStream<Cab> cabDataStream = env.addSource(new CabDataSource());
    //Popular destination.  | Where more number of people reach.
      cabDataStream
              .filter((FilterFunction<Cab>) value -> value.getOnGoing().equals("yes"))
              .keyBy((KeySelector<Cab, String>) Cab::getDestination)
              .sum("passengerCount")// use flat  map beacause out should should be returned only if you have more than 2 elements
              .print("Popular destination");
    //Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.
    cabDataStream
            .filter((FilterFunction<Cab>) value -> value.getOnGoing().equals("yes"))
            .keyBy((KeySelector<Cab, String>) Cab::getPickupLocation)
            .flatMap(new CalculateAveragePassengers()) // use flat  map because out should be returned only if you have more than 2 elements
            .print("Average number of passengers");
    //Average number of trips for a driver.
    cabDataStream
            .filter((FilterFunction<Cab>) value -> value.getOnGoing().equals("yes"))
            .keyBy((KeySelector<Cab, String>) Cab::getCabDriverName)
            .flatMap(new CalculateAverageTrip()) // use flat  map because out should be returned only if you have more than 2 elements
            .print("Average number of passengers");

    env.execute("CabDataStreamingApp");
  }


}
