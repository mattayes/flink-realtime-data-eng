package com.flinklearn.realtime.chapter6;

import com.flinklearn.realtime.common.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

/*
A Flink Program that reads a files stream, computes a Map and Reduce operation,
and writes to a file output
 */

public class CourseUseCase {

    private static final Logger LOG = LoggerFactory.getLogger(CourseUseCase.class);

    public static void main(String[] args) throws Exception {

        /*
         *                 Setup Flink environment.
         */

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        /*
         *                  Read CSV File Stream into a DataStream.
         */

        // Define the data directory to monitor for new files
        final String dataDir = "/data/raw_browser_events";

        // Define the text input source based on the directory
        final FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(dataDir))
                .monitorContinuously(Duration.ofSeconds(1))
                .build();

        // Create a DataStream based on the directory
        final DataStream<String> browserEventsStr = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        );

        // Convert each record to a Tuple
        final DataStream<Tuple3<String, String, Long>> browserEventsObj = browserEventsStr
                .map(new MapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(String eventStr) {
                        LOG.info("--- Received Record : {}", eventStr);

                        String[] columns = eventStr
                                .replace("\"", "")
                                .split(",");

                        return new Tuple3<>(
                                columns[1], //User
                                columns[2], //Action
                                Long.valueOf(columns[3])); //Timestamp
                    }
                });

        /*
         *                  By User / By Action 10 second Summary
         */

        final DataStream<Tuple3<String, String, Integer>> userActionSummary = browserEventsObj
                .map(new MapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> map(Tuple3<String, String, Long> x) {
                        return new Tuple3<>(x.f0, x.f1, 1); // Extract User,Action and Count 1
                    }
                })
                .keyBy(new KeySelector<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple3<String, String, Integer> x) {
                        return new Tuple2<>(x.f0, x.f1);
                    }
                }) // By User and Action
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10))) // 10-second window
                .reduce((x, y) -> new Tuple3<>(x.f0, x.f1, x.f2 + y.f2)); // Sum the counts

        // Pretty Print User Action 10 second Summary
        userActionSummary
                .map(new MapFunction<Tuple3<String, String, Integer>, Object>() {
                    @Override
                    public Object map(Tuple3<String, String, Integer> summary) {
                        LOG.info("User Action Summary :  User : {}, Action : {}, Total : {}", summary.f0, summary.f1, summary.f2);
                        return null;
                    }
                });

        /*
         *                  Find Duration of Each User Action
         */

        final DataStream<Tuple3<String, String, Long>> userActionDuration = browserEventsObj
                .keyBy(i -> i.f0)  // Key By User
                .map(new RichMapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {

                    //Keep track of last event name
                    private transient ValueState<String> lastEventName;
                    //Keep track of last event timestamp
                    private transient ValueState<Long> lastEventStart;

                    @Override
                    public void open(Configuration config) {

                        //Setup state Stores
                        ValueStateDescriptor<String> nameDescriptor = new ValueStateDescriptor<>(
                                "last-action-name", // the state name
                                TypeInformation.of(new TypeHint<>() {
                                }));

                        lastEventName = getRuntimeContext().getState(nameDescriptor);

                        ValueStateDescriptor<Long> startDescriptor = new ValueStateDescriptor<>(
                                "last-action-start", // the state name
                                TypeInformation.of(new TypeHint<>() {
                                }));

                        lastEventStart = getRuntimeContext().getState(startDescriptor);
                    }

                    @Override
                    public Tuple3<String, String, Long> map(Tuple3<String, String, Long> browserEvent) throws IOException {

                        // Default to publish
                        String publishAction = "None";
                        long publishDuration = 0L;

                        // If it's not the first event of the session and a login event, duration not applicable
                        if (lastEventName.value() != null && !browserEvent.f1.equals("Login")) {
                            // Set the last event name
                            publishAction = lastEventName.value();
                            // Last event duration = difference in timestamps
                            publishDuration = browserEvent.f2 - lastEventStart.value();
                        }

                        // If logout event, unset the state trackers
                        if (browserEvent.f1.equals("Logout")) {
                            lastEventName.clear();
                            lastEventStart.clear();
                        }
                        // Update the state trackers with current event
                        else {
                            lastEventName.update(browserEvent.f1);
                            lastEventStart.update(browserEvent.f2);
                        }
                        // Publish durations
                        return new Tuple3<>(browserEvent.f0, publishAction, publishDuration);
                    }
                });

        // Pretty Print
        userActionDuration
                .map(new MapFunction<Tuple3<String, String, Long>, Object>() {
                    @Override
                    public Object map(Tuple3<String, String, Long> summary) {
                        LOG.info("Durations :  User : {}, Action : {}, Duration : {}", summary.f0, summary.f1, summary.f2);
                        return null;
                    }
                });


        /*
         *                  Setup data source and execute the Flink pipeline
         */
        //Start the Browser Stream generator on a separate thread
        Utils.printHeader("Starting Browser Data Generator...");
        FileUtils.cleanDirectory(new File("data/raw_browser_events"));
        final Thread genThread = new Thread(new BrowserStreamDataGenerator());
        genThread.start();

        // execute the streaming pipeline
        env.execute("Flink Streaming Course Use Case Example");
    }
}
