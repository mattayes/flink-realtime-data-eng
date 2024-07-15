package com.flinklearn.realtime.chapter3;

import com.flinklearn.realtime.chapter2.AuditTrail;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import com.flinklearn.realtime.datasource.KafkaStreamDataGenerator;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class WindowJoins {

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                 Setup Flink environment.
             ****************************************************************************/

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                        = StreamExecutionEnvironment.getExecutionEnvironment();

            /****************************************************************************
             *                  Read CSV File Stream into a DataStream.
             ****************************************************************************/

            //Define the data directory to monitor for new files
            String dataDir = "data/raw_audit_trail";

            //Define the text input format based on the directory
            TextInputFormat auditFormat = new TextInputFormat(
                    new Path(dataDir));

            //Create a Datastream based on the directory
            DataStream<String> fileTrailStr
                        = streamEnv.readFile(auditFormat,
                            dataDir,    //Director to monitor
                            FileProcessingMode.PROCESS_CONTINUOUSLY,
                            1000); //monitor interval


            //Convert each record to an Object
            DataStream<AuditTrail> fileTrailObj
                    = fileTrailStr
                        .map(new MapFunction<String,AuditTrail>() {
                            @Override
                            public AuditTrail map(String auditStr) {
                                System.out.println("--- Received File Record : " + auditStr);
                                return new AuditTrail(auditStr);
                            }
                        });

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
            DataStream<String> kafkaTrailStr = streamEnv
                    .addSource(kafkaConsumer);

            //Convert each record to an Object
            DataStream<AuditTrail> kafkaTrailObj
                    = kafkaTrailStr
                    .map(new MapFunction<String,AuditTrail>() {
                        @Override
                        public AuditTrail map(String auditStr) {
                            System.out.println("--- Received Kafka Record : " + auditStr);
                            return new AuditTrail(auditStr);
                        }
                    });

            /****************************************************************************
             *                  Join both streams based on the same window
             ****************************************************************************/

             DataStream<Tuple2<String, Integer>> joinCounts =
                fileTrailObj.join(kafkaTrailObj) //Join the two streams

                     //WHERE used to select JOIN column from first Stream
                    .where(new KeySelector<AuditTrail, String>() {

                        @Override
                        public String getKey(AuditTrail auditTrail) throws Exception {
                            return auditTrail.getUser();
                        }
                    })
                     //EQUALTO used to select JOIN column from second stream
                    .equalTo(new KeySelector<AuditTrail, String>() {

                        @Override
                        public String getKey(AuditTrail auditTrail) throws Exception {
                            return auditTrail.getUser();
                        }
                    })
                     //Create a Tumbling window of 5 seconds
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))

                        //Apply JOIN function. Will be called for each matched
                        //combination of records.
                     .apply(new JoinFunction<AuditTrail, AuditTrail,
                             Tuple2<String,Integer>>() {
                         @Override
                         public Tuple2<String, Integer> join
                                 (AuditTrail fileTrail,
                                  AuditTrail kafkaTrail) throws Exception {

                             return new Tuple2<String, Integer>(
                                        fileTrail.getUser(), 1);
                         }
                     });

             //Print the counts
             joinCounts.print();

            /****************************************************************************
            *                  Setup data source and execute the Flink pipeline
            ****************************************************************************/
            //Start the File Stream generator on a separate thread
            Utils.printHeader("Starting File Data Generator...");
            Thread genThread = new Thread(new FileStreamDataGenerator());
            genThread.start();

            //Start the Kafka Stream generator on a separate thread
            Utils.printHeader("Starting Kafka Data Generator...");
            Thread kafkaThread = new Thread(new KafkaStreamDataGenerator());
            kafkaThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Streaming Window Joins Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
