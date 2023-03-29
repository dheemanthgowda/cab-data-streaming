package com.learning.flink.datastream.datasource;

import com.learning.flink.datastream.datamodels.Cab;
import java.io.File;
import java.util.Objects;
import java.util.Scanner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class CabDataSource extends RichSourceFunction<Cab> {

    private String filePath;


    @Override
    public void open(Configuration parameters) {
       //filePath = parameters.getString("inputPath", "");
        filePath = "/Users/dg072881/Documents/GitHub/cab-data-streaming/src/main/resource/cabTripData.txt";
    }

    @Override
    public void run(SourceContext<Cab> sourceContext) throws Exception {
        File file = new File(filePath);
        Scanner sc = new Scanner(file);

        while (sc.hasNextLine()) {
            String[] eachLine = sc.nextLine().split(",");
            final Cab input = new Cab(eachLine[0],
                    eachLine[1],
                    eachLine[2],
                    eachLine[3],
                    eachLine[4],
                    eachLine[5],
                    eachLine[6],
                    eachLine[7].equals("'null'") ? null : Integer.valueOf(eachLine[7]));
            sourceContext.collect(input);

        }
    }

    @Override
    public void cancel() {
    }
}
