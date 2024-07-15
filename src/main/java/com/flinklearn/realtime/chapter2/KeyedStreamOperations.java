package com.flinklearn.realtime.chapter2;

import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/*
A Flink Program to demonstrate working on keyed streams.
 */

public class KeyedStreamOperations {

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                 Setup Flink environment.
             ****************************************************************************/

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                        = StreamExecutionEnvironment.getExecutionEnvironment();

            System.out.println("\nTotal Parallel Task Slots : " + streamEnv.getParallelism() );

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

            /****************************************************************************
             *                 Key By User, find Running count by User
            ****************************************************************************/

            //Convert each record to a Tuple with user and a sum of duration
            DataStream<Tuple2<String, Integer>> userCounts
                    = auditTrailStr
                    .map(new MapFunction<String,Tuple2<String,Integer>>() {

                             @Override
                             public Tuple2<String,Integer> map(String auditStr) {
                                 System.out.println("--- Received Record : " + auditStr);
                                 AuditTrail at = new AuditTrail(auditStr);
                                 return new Tuple2<String,Integer>(at.user,at.duration);
                             }
                         })

                    .keyBy(0)  //By user name
                    .reduce((x,y) -> new Tuple2<String,Integer>( x.f0, x.f1 + y.f1));

            //Print User and Durations.
            userCounts.print();

            /****************************************************************************
             *                  Setup data source and execute the Flink pipeline
             ****************************************************************************/
            //Start the File Stream generator on a separate thread
            Utils.printHeader("Starting File Data Generator...");
            Thread genThread = new Thread(new FileStreamDataGenerator());
            genThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Streaming Keyed Stream Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
