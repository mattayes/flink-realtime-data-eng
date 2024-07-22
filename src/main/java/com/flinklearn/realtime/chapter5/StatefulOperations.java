package com.flinklearn.realtime.chapter5;

import com.flinklearn.realtime.chapter2.AuditTrail;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;

/*
A Flink Program to demonstrate working on keyed streams.
 */

public class StatefulOperations {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulOperations.class);

    public static void main(String[] args) throws Exception {

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

        // Define the text input format based on the directory
        final FileSource<String> auditSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataDir))
                .monitorContinuously(Duration.ofSeconds(1))
                .build();

        // Create a DataStream based on the directory
        final DataStream<String> auditTrailStr = env.fromSource(
                auditSource,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        );

        /*
         *                 Using simple Stateful Operations
         */

        // Convert each record to an Object
        final DataStream<Tuple3<String, String, Long>> auditTrailState = auditTrailStr
                .map(new MapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(String auditStr) {
                        LOG.info("--- Received Record : {}", auditStr);
                        AuditTrail auditTrail = new AuditTrail(auditStr);
                        return new Tuple3<>(
                                auditTrail.getUser(),
                                auditTrail.getOperation(),
                                auditTrail.getTimestamp());
                    }
                });

        // Measure the time interval between DELETE operations by the same User
        final DataStream<Tuple2<String, Long>> deleteIntervals = auditTrailState
                .keyBy(i -> i.f0)
                .map(new RichMapFunction<Tuple3<String, String, Long>, Tuple2<String, Long>>() {
                    private transient ValueState<Long> lastDelete;

                    @Override
                    public void open(Configuration config) {
                        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                                "last-delete", // the state name
                                TypeInformation.of(new TypeHint<>() {
                                }));

                        lastDelete = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Tuple2<String, Long> map(Tuple3<String, String, Long> auditTrail) throws Exception {
                        Tuple2<String, Long> retTuple = new Tuple2<>("No-Alerts", 0L);

                        // If two deletes were done by the same user within 10 seconds
                        if (auditTrail.f1.equals("Delete")) {
                            if (lastDelete.value() != null) {
                                long timeDiff = auditTrail.f2 - lastDelete.value();
                                if (timeDiff < 10000L) {
                                    retTuple = new Tuple2<>(auditTrail.f0, timeDiff);
                                }
                            }
                            lastDelete.update(auditTrail.f2);
                        }
                        // If no specific alert record was returned
                        return retTuple;
                    }
                })
                .filter(alert -> {
                    if (alert.f0.equals("No-Alerts")) {
                        return false;
                    }
                    LOG.warn("\n!! DELETE Alert Received : User {} executed 2 deletes within {} ms\n", alert.f0, alert.f1);
                    return true;
                });


        /*
         *                  Setup data source and execute the Flink pipeline
         */
        //Start the File Stream generator on a separate thread
        Utils.printHeader("Starting File Data Generator...");
        FileUtils.cleanDirectory(new File("data/raw_audit_trail"));
        final Thread genThread = new Thread(new FileStreamDataGenerator());
        genThread.start();

        // execute the streaming pipeline
        env.execute("Flink Streaming Stateful Operations Example");
    }
}
