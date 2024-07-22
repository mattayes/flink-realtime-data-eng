package com.flinklearn.realtime.chapter4;

import com.flinklearn.realtime.chapter2.AuditTrail;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Date;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class EventTimeOperations {

    private static final Logger LOG = LoggerFactory.getLogger(EventTimeOperations.class);

    public static void main(String[] args) throws Exception {

        /*
         *                 Setup Flink environment.
         */

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
         *                  Read CSV File Stream into a DataStream.
         */

        // Define the data directory to monitor for new files
        String dataDir = "/data/raw_audit_trail";

        // Define the text input format based on the directory
        FileSource<String> auditSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataDir))
                .monitorContinuously(Duration.ofSeconds(1))
                .build();

        // Create a DataStream based on the directory
        DataStream<String> auditTrailStr = env.fromSource(
                auditSource,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        );


        // Convert each record to an Object
        DataStream<AuditTrail> auditTrailObj = auditTrailStr.map(new MapFunction<String, AuditTrail>() {
            @Override
            public AuditTrail map(String auditStr) {
                LOG.info("--- Received Record : {}", auditStr);
                return new AuditTrail(auditStr);
            }
        });

        /*
         *                  Setup Event Time and Watermarks
         */
        // Create a watermarked DataStream
        DataStream<AuditTrail> auditTrailWithET = auditTrailObj.assignTimestampsAndWatermarks(new WatermarkStrategy<>() {
            @Override
            public WatermarkGenerator<AuditTrail> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<>() {
                    //Extract Watermark
                    transient long currWaterMark = 0L;

                    @Override
                    public void onEvent(AuditTrail auditTrail, long eventTimestamp, WatermarkOutput output) {
                        final long currentTime = System.currentTimeMillis();
                        final int delay = 10000;
                        if (currWaterMark == 0L) {
                            currWaterMark = currentTime;
                        }
                        // update watermark every 10 seconds
                        else if (currentTime - currWaterMark > delay) {
                            currWaterMark = currentTime;
                        }
                        // return watermark adjusted to buffer
                        final int buffer = 2000;
                        output.emitWatermark(new Watermark(currWaterMark - buffer));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                    }
                };
            }

            @Override
            public TimestampAssigner<AuditTrail> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return (auditTrail, recordTimestamp) -> auditTrail.getTimestamp();
            }
        });

        /*
         *                  Process a Watermarked Stream
         */

        // Create a Separate Trail for Late events
        final OutputTag<Tuple2<String, Integer>> lateAuditTrail = new OutputTag<>("late-audit-trail") {
        };

        SingleOutputStreamOperator<Tuple2<String, Integer>> finalTrail = auditTrailWithET
                .map(i -> new Tuple2<>(String.valueOf(i.getTimestamp()), 1)) // get event timestamp and count
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(1))) // Window by 1 second
                .sideOutputLateData(lateAuditTrail) // Handle late data
                .reduce((x, y) -> new Tuple2<>(x.f0, x.f1 + y.f1)) //Find total records every second
                // Pretty print
                .map(new MapFunction<Tuple2<String, Integer>,
                        Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> minuteSummary) {
                        String currentTime = (new Date()).toString();
                        String eventTime = (new Date(Long.parseLong(minuteSummary.f0))).toString();

                        LOG.info("Summary :  Current Time : {} Event Time : {} Count :{}", currentTime, eventTime, minuteSummary.f1);
                        return minuteSummary;
                    }
                });


        // Collect late events and process them later.
        DataStream<Tuple2<String, Integer>> lateTrail = finalTrail.getSideOutput(lateAuditTrail);


        /*
         *                  Send Processed Results to a Kafka Sink
         */

        // Create a sink for Kafka
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka-broker:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flink.kafka.streaming.sink")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setProperty("transaction.timeout.ms", "60000") // breaks without this
                .build();

        //Publish to Kafka
        finalTrail
                //Convert to String and write to Kafka
                .map(new MapFunction<Tuple2<String, Integer>, String>() {

                    @Override
                    public String map(Tuple2<String, Integer> finalTrail) {
                        return finalTrail.f0 + " = " + finalTrail.f1;
                    }
                })
                //Add Producer to Sink
                .sinkTo(kafkaSink);


        /*
         *                  Setup data source and execute the Flink pipeline
         */
        //Start the File Stream generator on a separate thread
        Utils.printHeader("Starting File Data Generator...");
        FileUtils.cleanDirectory(new File("data/raw_audit_trail"));
        Thread genThread = new Thread(new FileStreamDataGenerator());
        genThread.start();

        // execute the streaming pipeline
        env.execute("Flink Streaming Event Timestamp Example");
    }
}
