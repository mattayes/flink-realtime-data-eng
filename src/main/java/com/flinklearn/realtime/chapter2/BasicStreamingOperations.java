package com.flinklearn.realtime.chapter2;

import java.time.Duration;

import com.flinklearn.realtime.common.MapCountPrinter;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.DataDir;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class BasicStreamingOperations {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
         *                 Setup Flink environment.
         */

        // Keeps the ordering of records. Else multiple threads can change
        // sequence of printing.
        env.setParallelism(1);
        env.enableCheckpointing(1_000); // 1s

        /*
         *                  Read CSV File Stream into a DataStream.
         */

        // Define the text input source
        final String dataDir = "/data/raw_audit_trail";
        final FileSource<String> auditTrailSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataDir))
                .monitorContinuously(Duration.ofSeconds(1)) //monitor interval
                .build();

        // Create a DataStream based on the source
        final DataStream<String> auditTrailStream = env.fromSource(
                auditTrailSource,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        );

        // Convert each record to an Object
        final DataStream<AuditTrail> auditTrailObj = auditTrailStream
                .map( auditStr -> {
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
        final DataStream<Tuple2<String,Integer>> recCount = auditTrailObj
                .map(i -> new Tuple2<>(String.valueOf(System.currentTimeMillis()), 1))
                .returns(Types.TUPLE(Types.STRING ,Types.INT))
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                .reduce((x,y) -> new Tuple2<>(x.f0, x.f1 + y.f1));


        // Set up a streaming file sink to the output directory
        final String outputDir = "/data/five_sec_summary";
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
        DataDir.clean("data/raw_audit_trail");
        final Thread genThread = new Thread(new FileStreamDataGenerator());
        genThread.start();

        DataDir.clean("data/five_sec_summary");
        env.execute("Flink Streaming Audit Trail Example");
    }

}
