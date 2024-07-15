package com.flinklearn.realtime.chapter6;

import com.flinklearn.realtime.common.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class CourseUseCase {

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
            String dataDir = "data/raw_browser_events";

            //Define the text input format based on the directory
            TextInputFormat auditFormat = new TextInputFormat(
                    new Path(dataDir));

            //Create a Datastream based on the directory
            DataStream<String> browserEventsStr
                        = streamEnv.readFile(auditFormat,
                            dataDir,    //Director to monitor
                            FileProcessingMode.PROCESS_CONTINUOUSLY,
                            1000); //monitor interval

            //Convert each record to an Tuple
            DataStream<Tuple3<String,String,Long>> browserEventsObj
                    = browserEventsStr
                        .map(new MapFunction<String,Tuple3<String,String,Long>>() {
                            @Override
                            public Tuple3<String,String,Long> map(String eventStr) {

                                System.out.println("--- Received Record : " + eventStr);

                                String[] columns = eventStr
                                                    .replace("\"","")
                                                    .split(",");

                                return new Tuple3<String,String,Long>(
                                        columns[1], //User
                                        columns[2], //Action
                                        Long.valueOf(columns[3])); //Timestamp
                            }
                        });

            /****************************************************************************
             *                  By User / By Action 10 second Summary
            ****************************************************************************/

            DataStream<Tuple3<String,String,Integer>> userActionSummary =
                browserEventsObj
                        .map( x ->  new Tuple3<String,String,Integer>
                                        (x.f0, x.f1, 1)) //Extract User,Action and Count 1
                        .returns(Types.TUPLE(
                                Types.STRING ,Types.STRING, Types.INT))

                        .keyBy(0,1) //By User and Action

                        .timeWindow(Time.seconds(10)) //10 Second window

                        .reduce( (x,y) -> //Sum the counts
                                new Tuple3<String,String,Integer>
                                        (x.f0, x.f1, x.f2 + y.f2));

            //Pretty Print User Action 10 second Summary
            userActionSummary
                    .map(new MapFunction<Tuple3<String, String, Integer>, Object>() {
                        @Override
                        public Object map(Tuple3<String, String, Integer> summary) throws Exception {
                            System.out.println("User Action Summary : "
                                + " User : " + summary.f0
                                + ", Action : " + summary.f1
                                + ", Total : " + summary.f2);
                            return null;
                        }
                    });

            /****************************************************************************
             *                  Find Duration of Each User Action
             ****************************************************************************/

            DataStream<Tuple3<String,String,Long>> userActionDuration
                    = browserEventsObj
                    .keyBy(0)  //Key By User

                    .map(new RichMapFunction<Tuple3<String, String, Long>,
                            Tuple3<String, String, Long>>() {

                        //Keep track of last event name
                        private transient ValueState<String> lastEventName;
                        //Keep track of last event timestamp
                        private transient ValueState<Long> lastEventStart;

                        @Override
                        public void open(Configuration config) throws Exception{

                            //Setup state Stores
                            ValueStateDescriptor<String> nameDescriptor =
                                    new ValueStateDescriptor<String>(
                                            "last-action-name", // the state name
                                            TypeInformation.of(new TypeHint<String>() {}));

                            lastEventName = getRuntimeContext().getState(nameDescriptor);

                            ValueStateDescriptor<Long> startDescriptor =
                                    new ValueStateDescriptor<Long>(
                                            "last-action-start", // the state name
                                            TypeInformation.of(new TypeHint<Long>() {}));

                            lastEventStart = getRuntimeContext().getState(startDescriptor);
                        }

                        @Override
                        public Tuple3<String, String, Long> map(
                                Tuple3<String, String, Long> browserEvent) throws Exception {

                            //Default to publish
                            String publishAction = "None";
                            Long publishDuration = 0L;

                            //Check if its not the first event of the session
                            if (lastEventName.value() != null ) {

                                //If login event, duration not applicable
                                if ( ! browserEvent.f1.equals("Login")) {

                                    //Set the last event name
                                    publishAction = lastEventName.value();
                                    //Last event duration = difference in timestamps
                                    publishDuration = browserEvent.f2 - lastEventStart.value();
                                }
                            }

                            //If logout event, unset the state trackers
                            if ( browserEvent.f1.equals("Logout")) {
                                lastEventName.clear();
                                lastEventStart.clear();
                            }
                            //Update the state trackers with current event
                            else {
                                lastEventName.update(browserEvent.f1);
                                lastEventStart.update(browserEvent.f2);
                            }
                            //Publish durations
                            return new Tuple3<String,String,Long>(
                                    browserEvent.f0, publishAction, publishDuration);
                        }
                    });

            //Pretty Print
            userActionDuration
                    .map(new MapFunction<Tuple3<String, String, Long>, Object>() {
                        @Override
                        public Object map(Tuple3<String, String, Long> summary) throws Exception {
                            System.out.println("Durations : "
                                    + " User : " + summary.f0
                                    + ", Action : " + summary.f1
                                    + ", Duration : " + summary.f2);
                            return null;
                        }
                    });


            /****************************************************************************
             *                  Setup data source and execute the Flink pipeline
             ****************************************************************************/
            //Start the Browser Stream generator on a separate thread
            Utils.printHeader("Starting Browser Data Generator...");
            Thread genThread = new Thread(new BrowserStreamDataGenerator());
            genThread.start();

            // execute the streaming pipeline
            streamEnv.execute("Flink Streaming Course Use Case Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
