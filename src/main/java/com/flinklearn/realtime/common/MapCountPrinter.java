package com.flinklearn.realtime.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MapCountPrinter {

    public static void printCount(DataStream<Object> dsObj, String mesg) {

        dsObj
                //Generate a counter record for each input record
                .map( i
                    -> new Tuple2<String,Integer>
                        (mesg,1))
                .returns(Types.TUPLE(Types.STRING ,Types.INT))

                //Window by time = 5 seconds
                .timeWindowAll(Time.seconds(5))

                //Sum the number of records for each 5 second interval
                .reduce((x,y) ->
                        (new Tuple2<String, Integer>(x.f0, x.f1 + y.f1)))

                //Print the summary
                .map(new MapFunction<Tuple2<String,Integer>, Integer>(){

                    @Override
                    public Integer map(Tuple2<String, Integer> recCount) throws Exception {
                        Utils.printHeader(recCount.f0 + " : " + recCount.f1);
                        return recCount.f1;
                    }
                });
    }
}
