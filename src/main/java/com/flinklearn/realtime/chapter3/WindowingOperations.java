package com.flinklearn.realtime.chapter3;

import com.flinklearn.realtime.chapter2.AuditTrail;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.KafkaStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class WindowingOperations {

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                 Setup Flink environment.
             ****************************************************************************/

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                        = StreamExecutionEnvironment.getExecutionEnvironment();

            /****************************************************************************
             *                  Read Kafka Topic Stream into a DataStream.
             ****************************************************************************/

            //Set connection properties to Kafka Cluster
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            properties.setProperty("group.id", "flink.learn.realtime");

            //Setup a Kafka Consumer on Flnk
            FlinkKafkaConsumer<String> kafkaConsumer =
                    new FlinkKafkaConsumer<>
                            ("flink.kafka.streaming.source", //topic
                                    new SimpleStringSchema(), //Schema for data
                                    properties); //connection properties

            //Setup to receive only new messages
            kafkaConsumer.setStartFromLatest();

            //Create the data stream
            DataStream<String> auditTrailStr = streamEnv
                    .addSource(kafkaConsumer);

            //Convert each record to an Object
            DataStream<AuditTrail> auditTrailObj
                    = auditTrailStr
                    .map(new MapFunction<String,AuditTrail>() {
                        @Override
                        public AuditTrail map(String auditStr) {
                            System.out.println("--- Received Record : " + auditStr);
                            return new AuditTrail(auditStr);
                        }
                    });

            /****************************************************************************
             *                  Use Sliding Windows.
             ****************************************************************************/

            //Compute the count of events, minimum timestamp and maximum timestamp
            //for a sliding window interval of 10 seconds, sliding by 5 seconds
            DataStream<Tuple4<String, Integer, Long, Long>>
                    slidingSummary
                        = auditTrailObj
                    .map(i
                            -> new Tuple4<String, Integer, Long, Long>
                            (String.valueOf(System.currentTimeMillis()), //Current Time
                                    1,      //Count each Record
                                    i.getTimestamp(),   //Minimum Timestamp
                                    i.getTimestamp()))  //Maximum Timestamp
                    .returns(Types.TUPLE(Types.STRING,
                            Types.INT,
                            Types.LONG,
                            Types.LONG))
                    .timeWindowAll(
                            Time.seconds(10), //Window Size
                            Time.seconds(5))  //Slide by 5
                    .reduce((x, y)
                            -> new Tuple4<String, Integer, Long, Long>(
                            x.f0,
                            x.f1 + y.f1,
                            Math.min(x.f2, y.f2),
                            Math.max(x.f3, y.f3)));

            //Pretty Print the tuples
            slidingSummary.map(new MapFunction<Tuple4<String, Integer, Long, Long>, Object>() {

                @Override
                public Object map(Tuple4<String, Integer, Long, Long> slidingSummary)
                        throws Exception {

                    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

                    String minTime
                            = format.format(new Date(Long.valueOf(slidingSummary.f2)));

                    String maxTime
                            = format.format(new Date(Long.valueOf(slidingSummary.f3)));

                    System.out.println("Sliding Summary : "
                        + (new Date()).toString()
                        + " Start Time : " + minTime
                        + " End Time : " + maxTime
                        + " Count : " + slidingSummary.f1);

                    return null;
                }
            });

            /****************************************************************************
             *                  Use Session Windows.
             ****************************************************************************/

            //Execute the same example as before using Session windows
            //Partition by User and use a window gap of 5 seconds.
            DataStream<Tuple4<String, Integer, Long, Long>>
                    sessionSummary
                    = auditTrailObj
                    .map(i
                            -> new Tuple4<String, Integer, Long, Long>
                            (i.getUser(), //Get user
                                    1,      //Count each Record
                                    i.getTimestamp(),   //Minimum Timestamp
                                    i.getTimestamp()))  //Maximum Timestamp
                    .returns(Types.TUPLE(Types.STRING,
                            Types.INT,
                            Types.LONG,
                            Types.LONG))

                    .keyBy(0) //Key by user

                    .window(ProcessingTimeSessionWindows
                            .withGap(Time.seconds(5)))

                    .reduce((x, y)
                            -> new Tuple4<String, Integer, Long, Long>(
                            x.f0,
                            x.f1 + y.f1,
                            Math.min(x.f2, y.f2),
                            Math.max(x.f3, y.f3)));

            //Pretty print
            sessionSummary.map(new MapFunction<Tuple4<String, Integer,
                    Long, Long>, Object>() {

                @Override
                public Object map(Tuple4<String, Integer, Long, Long> sessionSummary)
                        throws Exception {

                    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

                    String minTime
                            = format.format(new Date(Long.valueOf(sessionSummary.f2)));

                    String maxTime
                            = format.format(new Date(Long.valueOf(sessionSummary.f3)));

                    System.out.println("Session Summary : "
                            + (new Date()).toString()
                            + " User : " + sessionSummary.f0
                            + " Start Time : " + minTime
                            + " End Time : " + maxTime
                            + " Count : " + sessionSummary.f1);

                    return null;
                }
            });

            /****************************************************************************
             *                  Setup data source and execute the Flink pipeline
             ****************************************************************************/
            //Start the Kafka Stream generator on a separate thread
            Utils.printHeader("Starting Kafka Data Generator...");
            Thread kafkaThread = new Thread(new KafkaStreamDataGenerator());
            kafkaThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Windowing Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
