package com.flinklearn.realtime.chapter2;

import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.DataDir;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/*
A Flink Program to demonstrate working on keyed streams.
 */

public class KeyedStreamOperations {

    public static void main(String[] args) throws Exception {

        /*
         *                 Setup Flink environment.
        */

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env
                    = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("\nTotal Parallel Task Slots : " + env.getParallelism() );

        /*
         *                  Read CSV File Stream into a DataStream.
         */

        // Define the data directory to monitor for new files
        final String dataDir = "/data/raw_audit_trail";

        // Define the text input format based on the directory
        final FileSource<String> auditSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataDir))
                .monitorContinuously(Duration.ofSeconds(1))
                .build();

        // Create a DataStream
        final DataStream<String> auditTrailStream = env.fromSource(
                auditSource,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        );

        /*
         *                 Key By User, find Running count by User
        */

        // Convert each record to a Tuple with user and a sum of duration
        final DataStream<Tuple2<String, Integer>> userCounts
                = auditTrailStream
                .map((MapFunction<String, Tuple2<String, Integer>>) auditStr -> {
                    System.out.println("--- Received Record : " + auditStr);
                    AuditTrail at = new AuditTrail(auditStr);
                    return new Tuple2<>(at.user, at.duration);
                })
                .keyBy(i -> i.f0)  // By username
                .reduce((x,y) -> new Tuple2<>(x.f0, x.f1 + y.f1));

        // Print User and Durations.
        userCounts.print();

        /*
         *                  Setup data source and execute the Flink pipeline
         */
        // Start the File Stream generator on a separate thread
        Utils.printHeader("Starting File Data Generator...");
        DataDir.clean("data/raw_audit_trail");
        final Thread genThread = new Thread(new FileStreamDataGenerator());
        genThread.start();

        // execute the streaming pipeline
        env.execute("Flink Streaming Keyed Stream Example");
    }

}
