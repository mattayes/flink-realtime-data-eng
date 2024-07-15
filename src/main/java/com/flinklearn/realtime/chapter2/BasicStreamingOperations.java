package com.flinklearn.realtime.chapter2;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

import com.flinklearn.realtime.common.MapCountPrinter;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class BasicStreamingOperations {

    private static final Logger LOG = LoggerFactory.getLogger(BasicStreamingOperations.class);

    public static void main(String[] args) {
        /*
         *                 Setup Flink environment.
         */

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Keeps the ordering of records. Else multiple threads can change
        // sequence of printing.
        env.setParallelism(1);

        /*
         *                  Read CSV File Stream into a DataStream.
         */

        // Define the data directory to monitor for new files
        final String dataDir = "/data/raw_audit_trail";

        // Define the text input source
        final FileSource<String> auditTrailSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataDir))
                .monitorContinuously(Duration.ofSeconds(60L)) //monitor interval
                .build();

        // Create a DataStream based on the source
        final DataStream<String> auditTrailStream = env.fromSource(
                auditTrailSource,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        );

        // Convert each record to an Object
        final DataStream<AuditTrail> auditTrailObj = auditTrailStream
                .map((MapFunction<String, AuditTrail>) auditStr -> {
                    System.out.println("--- Received Record : " + auditStr);
                    return new AuditTrail(auditStr);
                });

        /*
         *                  Perform computations and write to output sink.
        */

        // Print message for audit trail counts
        MapCountPrinter.printCount(
                auditTrailObj.map( i -> i),
                "Audit Trail : Last 5 secs");

        // Window by 5 seconds, count #of records and save to output
       final DataStream<Tuple2<String,Integer>> recCount
                = auditTrailObj
                .map(i -> new Tuple2<>(String.valueOf(System.currentTimeMillis()), 1))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((x,y) -> new Tuple2<>(x.f0, x.f1 + y.f1));


        // Define the output directory to store summary information
        final String outputDir = "/data/five_sec_summary";
        // Clean out existing files in the directory
        try {
            FileUtils.cleanDirectory(new File(outputDir));
        } catch (IOException e) {
            LOG.error("Error cleaning directory", e);
        }

        // Set up a streaming file sink to the output directory
        final FileSink<Tuple2<String,Integer>> countSink = FileSink
                .forRowFormat(
                        new Path(outputDir),
                        new SimpleStringEncoder<Tuple2<String,Integer>>("UTF-8")
                ).build();

        // Add the file sink as sink to the DataStream.
        recCount.sinkTo(countSink);


        /*
         *                  Setup data source and execute the Flink pipeline
         */
        // Start the File Stream generator on a separate thread
        Utils.printHeader("Starting File Data Generator...");
        final Thread genThread = new Thread(new FileStreamDataGenerator());
        genThread.start();

        // execute the streaming pipeline
        try {
            env.execute("Flink Streaming Audit Trail Example");
        } catch (Exception e) {
            LOG.error("Error running pipeline", e);
        }
    }

}
