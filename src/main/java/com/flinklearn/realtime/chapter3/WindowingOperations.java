package com.flinklearn.realtime.chapter3;

import com.flinklearn.realtime.chapter2.AuditTrail;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.KafkaStreamDataGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class WindowingOperations {

    private static final Logger LOG = LoggerFactory.getLogger(WindowingOperations.class);

    public static void main(String[] args) throws Exception {

        /*
         *                 Setup Flink environment.
         */

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
         *                  Read Kafka Topic Stream into a DataStream.
         */

        //Setup a Kafka Consumer on Flink
        KafkaSource<String> kafkaConsumer = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-broker:9092")
                .setGroupId("flink.learn.realtime")
                .setTopics("flink.kafka.streaming.source")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest()) // Setup to receive only new messages
                .build();

        //Create the data stream
        DataStream<String> auditTrailStr = env.fromSource(
                kafkaConsumer,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        //Convert each record to an Object
        DataStream<AuditTrail> auditTrailObj = auditTrailStr.map(new MapFunction<String, AuditTrail>() {
            @Override
            public AuditTrail map(String auditStr) {
                LOG.info("--- Received Record : {}", auditStr);
                return new AuditTrail(auditStr);
            }
        });

        /*
         *                  Use Sliding Windows.
         */

        //Compute the count of events, minimum timestamp and maximum timestamp
        //for a sliding window interval of 10 seconds, sliding by 5 seconds
        DataStream<Tuple4<String, Integer, Long, Long>> slidingSummary = auditTrailObj
                .map(i -> new Tuple4<>(
                        String.valueOf(System.currentTimeMillis()), //Current Time
                        1,      //Count each Record
                        i.getTimestamp(),   //Minimum Timestamp
                        i.getTimestamp())) //Maximum Timestamp
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.LONG))
                .windowAll(SlidingProcessingTimeWindows.of(
                        Duration.ofSeconds(10), //Window Size
                        Duration.ofSeconds(5))) //Slide by 5
                .reduce((x, y) -> new Tuple4<>(
                        x.f0,
                        x.f1 + y.f1,
                        Math.min(x.f2, y.f2),
                        Math.max(x.f3, y.f3))
                );

        //Pretty Print the tuples
        slidingSummary.map(new MapFunction<Tuple4<String, Integer, Long, Long>, Object>() {
            @Override
            public Object map(Tuple4<String, Integer, Long, Long> slidingSummary) {
                SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
                String minTime = format.format(new Date(slidingSummary.f2));
                String maxTime = format.format(new Date(slidingSummary.f3));

                LOG.info("Sliding Summary : {} Start Time : {} End Time : {} Count : {}",
                        new Date(), minTime, maxTime, slidingSummary.f1);

                return null;
            }
        });

        /*
         *                  Use Session Windows.
         */

        //Execute the same example as before using Session windows
        //Partition by User and use a window gap of 5 seconds.
        DataStream<Tuple4<String, Integer, Long, Long>> sessionSummary = auditTrailObj
                .map(i -> new Tuple4<>(
                        i.getUser(),        // Get user
                        1,                  // Count each Record
                        i.getTimestamp(),   // Minimum Timestamp
                        i.getTimestamp()))   // Maximum Timestamp
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.LONG))
                .keyBy(i -> i.f0) // Key by user
                .window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(5)))
                .reduce((x, y) -> new Tuple4<>(
                        x.f0,
                        x.f1 + y.f1,
                        Math.min(x.f2, y.f2),
                        Math.max(x.f3, y.f3)));

        // Pretty print
        sessionSummary.map(new MapFunction<Tuple4<String, Integer, Long, Long>, Object>() {

            @Override
            public Object map(Tuple4<String, Integer, Long, Long> sessionSummary) {
                SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
                String minTime = format.format(new Date(sessionSummary.f2));
                String maxTime = format.format(new Date(sessionSummary.f3));

                LOG.info("Session Summary : {} User : {} Start Time : {} End Time : {} Count : {}",
                        new Date(), sessionSummary.f0, minTime, maxTime, sessionSummary.f1);

                return null;
            }
        });

        /*
         *                  Setup data source and execute the Flink pipeline
         */
        // Start the Kafka Stream generator on a separate thread
        Utils.printHeader("Starting Kafka Data Generator...");
        Thread kafkaThread = new Thread(new KafkaStreamDataGenerator());
        kafkaThread.start();

        // execute the streaming pipeline
        env.execute("Flink Windowing Example");
    }

}
