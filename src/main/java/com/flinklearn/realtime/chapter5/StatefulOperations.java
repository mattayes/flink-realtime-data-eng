package com.flinklearn.realtime.chapter5;

import com.flinklearn.realtime.chapter2.AuditTrail;
import com.flinklearn.realtime.common.MapCountPrinter;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/*
A Flink Program to demonstrate working on keyed streams.
 */

public class StatefulOperations {

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                 Setup Flink environment.
             ****************************************************************************/

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                        = StreamExecutionEnvironment.getExecutionEnvironment();

            //Keeps the ordering of records. Else multiple threads can change
            //sequence of printing.
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

            /****************************************************************************
             *                 Using simple Stateful Operations
            ****************************************************************************/

            //Convert each record to an Object
            DataStream<Tuple3<String,String, Long>> auditTrailState
                    = auditTrailStr
                    .map(new MapFunction<String,Tuple3<String,String, Long>>() {
                        @Override
                        public Tuple3<String,String, Long> map(String auditStr) {
                            System.out.println("--- Received Record : " + auditStr);
                            AuditTrail auditTrail = new AuditTrail(auditStr);
                            return new Tuple3<String,String, Long>(
                                    auditTrail.getUser(),
                                    auditTrail.getOperation(),
                                    auditTrail.getTimestamp());
                        }
                    });

            //Measure the time interval between DELETE operations by the same User
            DataStream<Tuple2<String, Long>> deleteIntervals
                    = auditTrailState
                    .keyBy(0)
                    .map(new RichMapFunction<Tuple3<String,String,Long>, Tuple2<String, Long>>() {

                        private transient ValueState<Long> lastDelete;

                        @Override
                        public void open(Configuration config) throws Exception{

                            ValueStateDescriptor<Long> descriptor =
                                    new ValueStateDescriptor<Long>(
                                            "last-delete", // the state name
                                            TypeInformation.of(new TypeHint<Long>() {}));

                            lastDelete = getRuntimeContext().getState(descriptor);

                        }

                        @Override
                        public Tuple2<String, Long>
                            map(Tuple3<String,String, Long> auditTrail) throws Exception {

                            Tuple2<String,Long> retTuple
                                    = new Tuple2<String,Long>("No-Alerts",0L);

                            //If two deletes were done by the same user within 10 seconds
                            if ( auditTrail.f1.equals("Delete")) {

                                if ( lastDelete.value() != null) {

                                    long timeDiff
                                            = auditTrail.f2
                                                - lastDelete.value();

                                    if ( timeDiff < 10000L) {
                                        retTuple = new Tuple2<String,Long>(
                                                auditTrail.f0,timeDiff);
                                    }
                                }
                                lastDelete.update(auditTrail.f2);
                            }
                            //If no specific alert record was returned
                            return retTuple;
                        }
                    })
                    .filter(new FilterFunction<Tuple2<String, Long>>() {
                        @Override
                        public boolean filter(Tuple2<String, Long> alert) throws Exception {

                            if ( alert.f0.equals("No-Alerts")) {
                                return false;
                            }
                            else {
                                System.out.println("\n!! DELETE Alert Received : User "
                                        + alert.f0 + " executed 2 deletes within "
                                        + alert.f1 + " ms" + "\n");
                                return true;
                            }
                        }
                    });


            /****************************************************************************
             *                  Setup data source and execute the Flink pipeline
             ****************************************************************************/
            //Start the File Stream generator on a separate thread
            Utils.printHeader("Starting File Data Generator...");
            Thread genThread = new Thread(new FileStreamDataGenerator());
            genThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Streaming Stateful Operations Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
