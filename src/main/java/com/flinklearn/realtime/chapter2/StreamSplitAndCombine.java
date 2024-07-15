package com.flinklearn.realtime.chapter2;

import com.flinklearn.realtime.common.MapCountPrinter;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/*
A Flink Program to demonstrate working on keyed streams.
 */

public class StreamSplitAndCombine {

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
             *         Split the Stream into two Streams based on Entity
            ****************************************************************************/


            //Create a Separate Trail for Sales Rep operations
            final OutputTag<Tuple2<String,Integer>> salesRepTag
                    = new OutputTag<Tuple2<String,Integer>>("sales-rep"){};

            //Convert each record to an Object
            SingleOutputStreamOperator<AuditTrail> customerTrail
                    = auditTrailStr
                        .process(new ProcessFunction<String,AuditTrail>() {

                            @Override
                            public void processElement(
                                            String auditStr,
                                            Context ctx,
                                            Collector<AuditTrail> collAudit) {

                                System.out.println("--- Received Record : " + auditStr);

                                //Convert String to AuditTrail Object
                                AuditTrail auditTrail = new AuditTrail(auditStr);

                                //Create output tuple with User and count
                                Tuple2<String,Integer> entityCount
                                        = new Tuple2<String,Integer>
                                                (auditTrail.user, 1);

                                if (auditTrail.getEntity().equals("Customer")) {
                                    //Collect main output for Customer as AuditTrail
                                    collAudit.collect(auditTrail);
                                }
                                else {
                                    //Collect side output for Sales Rep
                                    ctx.output(salesRepTag, entityCount);
                                }
                            }
                        });

            //Convert side output into a data stream
            DataStream<Tuple2<String,Integer>> salesRepTrail
                            = customerTrail.getSideOutput(salesRepTag);

            //Print Customer Record summaries
            MapCountPrinter.printCount(
                    customerTrail.map( i -> (Object)i),
                    "Customer Records in Trail : Last 5 secs");

            //Print Sales Rep Record summaries
            MapCountPrinter.printCount(
                    salesRepTrail.map( i -> (Object)i),
                    "Sales Rep Records in Trail : Last 5 secs");

            /****************************************************************************
             *         Combine two streams into one
             ****************************************************************************/

            ConnectedStreams<AuditTrail,Tuple2<String,Integer>> mergedTrail
                    = customerTrail
                        .connect(salesRepTrail);


            DataStream<Tuple3<String,String,Integer>> processedTrail
                = mergedTrail.map(new CoMapFunction<
                                    AuditTrail, //Stream 1
                                    Tuple2<String,Integer>, //Stream 2
                                    Tuple3<String,String,Integer> //Output
                                            >() {

                        @Override
                        public Tuple3<String,String,Integer>  //Process Stream 1
                            map1(AuditTrail auditTrail) throws Exception {
                                return new Tuple3<String,String,Integer>
                                        ("Stream-1", auditTrail.user,1);
                            }

                        @Override
                        public Tuple3<String,String,Integer> //Process Stream 2
                            map2(Tuple2<String, Integer> srTrail) throws Exception {
                                return new Tuple3<String,String,Integer>
                                        ("Stream-2", srTrail.f0,1);
                            }
                    });

            //Print the combined data stream

            processedTrail
                    .map(new MapFunction<Tuple3<String,String,Integer>,
                            Tuple3<String,String,Integer> >() {
                        @Override
                        public Tuple3<String,String,Integer>
                            map(Tuple3<String, String, Integer> user) {
                            System.out.println("--- Merged Record for User: " + user);
                            return null;
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
            streamEnv.execute("Flink Streaming Keyed Stream Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
