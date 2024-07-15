package com.flinklearn.realtime.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.slf4j.Logger;

import java.time.Duration;

public class MapCountPrinter {

    public static void printCount(Logger log, DataStream<Object> dsObj, String msg) {

        dsObj
                // Generate a counter record for each input record
                .map(new MapFunction<Object, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Object value) {
                        return new Tuple2<>(msg, 1);
                    }
                })
                // Window by time = 5 seconds
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                // Sum the number of records for each 5 second interval
                .reduce((x, y) -> new Tuple2<>(x.f0, x.f1 + y.f1))
                // Print the summary
                .map(new MapFunction<Tuple2<String, Integer>, Integer>() {
                    @Override
                    public Integer map(Tuple2<String, Integer> recCount) {
                        Utils.printHeader(log, recCount.f0 + " : " + recCount.f1);
                        return recCount.f1;
                    }
                });
    }
}
