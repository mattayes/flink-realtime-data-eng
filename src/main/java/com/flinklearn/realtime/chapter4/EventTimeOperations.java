package com.flinklearn.realtime.chapter4;

import com.flinklearn.realtime.chapter2.AuditTrail;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Properties;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class EventTimeOperations {

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                 Setup Flink environment.
             ****************************************************************************/

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                        = StreamExecutionEnvironment.getExecutionEnvironment();

            streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            streamEnv.setParallelism(1);

            /****************************************************************************
             *                  Read CSV File Stream into a DataStream.
             ****************************************************************************/

            //Define the data directory to monitor for new files
            String dataDir = "data/raw_audit_trail";

            //Define the text input format based on the directory
            TextInputFormat auditFormat = new TextInputFormat(
                    new Path(dataDir));

            //Create a Datastream based on the directory
            DataStream<String> auditTrailStr
                        = streamEnv.readFile(auditFormat,
                            dataDir,    //Director to monitor
                            FileProcessingMode.PROCESS_CONTINUOUSLY,
                            1000); //monitor interval


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
             *                  Setup Event Time and Watermarks
             ****************************************************************************/
            //Create a water marked Data Stream
            DataStream<AuditTrail> auditTrailWithET
                    =auditTrailObj.assignTimestampsAndWatermarks(
                            (new AssignerWithPunctuatedWatermarks<AuditTrail>() {

                            //Extract Event timestamp value.
                            @Override
                            public long extractTimestamp(
                                    AuditTrail auditTrail,
                                    long previousTimeStamp) {

                                return auditTrail.getTimestamp();
                                }

                            //Extract Watermark
                            transient long currWaterMark = 0L;
                            int delay = 10000;
                            int buffer = 2000;

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(
                                    AuditTrail auditTrail,
                                    long newTimestamp) {

                                long currentTime = System.currentTimeMillis();
                                if (currWaterMark == 0L) {
                                    currWaterMark = currentTime;
                                }
                                //update watermark every 10 seconds
                                else if ( currentTime - currWaterMark > delay) {
                                    currWaterMark = currentTime;
                                }
                                //return watermark adjusted to bufer
                                return new Watermark(
                                        currWaterMark - buffer);

                            }

                        }));

            /****************************************************************************
             *                  Process a Watermarked Stream
             ***************************************************************************/

            //Create a Separate Trail for Late events
            final OutputTag<Tuple2<String,Integer>> lateAuditTrail
                    = new OutputTag<Tuple2<String,Integer>>("late-audit-trail"){};

            SingleOutputStreamOperator<Tuple2<String, Integer>> finalTrail

                    = auditTrailWithET

                        .map(i -> new Tuple2<String, Integer> //get event timestamp and count
                                (String.valueOf(i.getTimestamp()), 1))
                        .returns(Types.TUPLE(Types.STRING, Types.INT))

                        .timeWindowAll(Time.seconds(1)) //Window by 1 second

                        .sideOutputLateData(lateAuditTrail) //Handle late data

                        .reduce((x, y) -> //Find total records every second
                                (new Tuple2<String, Integer>(x.f0, x.f1 + y.f1)))

                        //Pretty print
                        .map(new MapFunction<Tuple2<String, Integer>,
                                Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer>
                            map(Tuple2<String, Integer> minuteSummary)
                                    throws Exception {

                                String currentTime = (new Date()).toString();
                                String eventTime
                                        = (new Date(Long.valueOf(minuteSummary.f0))).toString();

                                System.out.println("Summary : "
                                        + " Current Time : " + currentTime
                                        + " Event Time : " + eventTime
                                        + " Count :" + minuteSummary.f1);

                                return minuteSummary;
                            }
                        });


            //Collect late events and process them later.
            DataStream<Tuple2<String, Integer>> lateTrail
                    =  finalTrail.getSideOutput(lateAuditTrail);


            /****************************************************************************
             *                  Send Processed Results to a Kafka Sink
             ****************************************************************************/

            //Setup Properties for Kafka connection
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");

            //Create a Producer for Kafka
            FlinkKafkaProducer<String> kafkaProducer
                    = new FlinkKafkaProducer<String>(
                            //Topic Name
                            "flink.kafka.streaming.sink",

                            //Serialization for String data.
                            (new KafkaSerializationSchema<String>() {

                                @Override
                                public ProducerRecord<byte[], byte[]>
                                    serialize(String s, @Nullable Long aLong) {

                                    return (new ProducerRecord<byte[],byte[] >
                                            ("flink.kafka.streaming.sink",
                                                    s.getBytes()));
                                }
                            }),

                            properties,
                            FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

            //Publish to Kafka
            finalTrail //Convert to String and write to Kafka
                    .map(new MapFunction<Tuple2<String, Integer>, String>() {

                        @Override
                        public String map(Tuple2<String, Integer> finalTrail)
                                throws Exception {

                            return finalTrail.f0 + " = " + finalTrail.f1;
                        }
                    })
                    //Add Producer to Sink
                    .addSink(kafkaProducer);


            /****************************************************************************
             *                  Setup data source and execute the Flink pipeline
             ****************************************************************************/
            //Start the File Stream generator on a separate thread
            Utils.printHeader("Starting File Data Generator...");
            Thread genThread = new Thread(new FileStreamDataGenerator());
            genThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Streaming Event Timestamp Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
