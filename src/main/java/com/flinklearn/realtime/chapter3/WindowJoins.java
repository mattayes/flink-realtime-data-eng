package com.flinklearn.realtime.chapter3;

import com.flinklearn.realtime.chapter2.AuditTrail;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import com.flinklearn.realtime.datasource.KafkaStreamDataGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.io.File;
import java.time.Duration;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class WindowJoins {

    public static void main(String[] args) throws Exception {

        /*
         *                 Setup Flink environment.
         */

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
         *                  Read CSV File Stream into a DataStream.
         */

        //Define the data directory to monitor for new files
        String dataDir = "/data/raw_audit_trail";

        //Define the text input format based on the directory
        final FileSource<String> auditSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataDir))
                .monitorContinuously(Duration.ofSeconds(1)) // monitor interval
                .build();

        //Create a DataStream based on the directory
        DataStream<String> fileTrailStr = env.fromSource(
                auditSource,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        );


        //Convert each record to an AuditTrail
        DataStream<AuditTrail> fileTrailObj = fileTrailStr.map(new MapFunction<String, AuditTrail>() {
            @Override
            public AuditTrail map(String auditStr) {
                System.out.println("--- Received File Record : " + auditStr);
                return new AuditTrail(auditStr);
            }
        });

        /*
         *                  Read Kafka Topic Stream into a DataStream.
         */

        // Set up a Kafka source on Flink
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-broker:9092")
                .setGroupId("flink.learn.realtime")
                .setTopics("flink.kafka.streaming.source")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest()) // Setup to receive only new messages
                .build();

        //Create the data stream
        DataStream<String> kafkaTrailStr = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Convert each record to an AuditTrail
        DataStream<AuditTrail> kafkaTrailObj = kafkaTrailStr
                .map(new MapFunction<String, AuditTrail>() {
                    @Override
                    public AuditTrail map(String auditStr) {
                        System.out.println("--- Received Kafka Record : " + auditStr);
                        return new AuditTrail(auditStr);
                    }
                });

        /*
         *                  Join both streams based on the same window
         */

        DataStream<Tuple2<String, Integer>> joinCounts = fileTrailObj
                .join(kafkaTrailObj) //Join the two streams
                //WHERE used to select JOIN column from first Stream
                .where((KeySelector<AuditTrail, String>) AuditTrail::getUser)
                // EQUALTO used to select JOIN column from second stream
                .equalTo((KeySelector<AuditTrail, String>) AuditTrail::getUser)
                //Create a Tumbling window of 5 seconds
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                //Apply JOIN function. Will be called for each matched
                //combination of records.
                .apply(new JoinFunction<AuditTrail, AuditTrail, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> join(AuditTrail fileTrail, AuditTrail kafkaTrail) {
                        return new Tuple2<>(fileTrail.getUser(), 1);
                    }
                });

        //Print the counts
        joinCounts.print();

        /*
         *                  Setup data source and execute the Flink pipeline
         */
        //Start the File Stream generator on a separate thread
        Utils.printHeader("Starting File Data Generator...");
        FileUtils.cleanDirectory(new File("data/raw_audit_trail"));
        Thread genThread = new Thread(new FileStreamDataGenerator());
        genThread.start();

        //Start the Kafka Stream generator on a separate thread
        Utils.printHeader("Starting Kafka Data Generator...");
        Thread kafkaThread = new Thread(new KafkaStreamDataGenerator());
        kafkaThread.start();

        // execute the streaming pipeline
        env.execute("Flink Streaming Window Joins Example");
    }
}
