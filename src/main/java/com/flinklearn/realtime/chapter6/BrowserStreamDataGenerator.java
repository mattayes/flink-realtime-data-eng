package com.flinklearn.realtime.chapter6;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/****************************************************************************
 * This Generator generates a series of data files in the raw_data folder
 * It is an audit trail data source.
 * This can be used for streaming consumption of data by Flink
 ****************************************************************************/

public class BrowserStreamDataGenerator implements Runnable {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_BLUE = "\u001B[34m";

    public static void main(String[] args) {
        BrowserStreamDataGenerator fsdg = new BrowserStreamDataGenerator();
        fsdg.run();
    }

    public void run() {

        //Define list of users
        final List<String> appUser = new ArrayList<>();
        appUser.add("Tom");
        appUser.add("Harry");
        appUser.add("Bob");

        //Define list of application operations
        final List<String> appOperation = new ArrayList<>();
        appOperation.add("Login");
        appOperation.add("ViewVideo");
        appOperation.add("ViewLink");
        appOperation.add("ViewReview");
        appOperation.add("Logout");

        // Define the data directory to output the files
        final String dataDir = "data/raw_browser_events";

        // Define a random number generator
        final Random random = new Random();

        // Generate 100 sample audit records, one per each file
        for (int i = 0; i < 100; i++) {

            // Capture current timestamp
            final String currentTime = String.valueOf(System.currentTimeMillis());

            // Generate a random user
            final String user = appUser.get(random.nextInt(appUser.size()));
            // Generate a random operation
            final String operation = appOperation.get(random.nextInt(appOperation.size()));
            // Generate a random entity

            // Create a CSV Text array
            final String[] csvText = {String.valueOf(i), user,
                    operation, currentTime};

            // Open a new file for this record
            FileWriter auditFile;
            try {
                auditFile = new FileWriter(dataDir
                        + "/browser_events" + i + ".csv");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            final CSVWriter auditCSV = new CSVWriter(auditFile);

            // Write the audit record and close the file
            auditCSV.writeNext(csvText);

            System.out.println(ANSI_BLUE + "Browser Stream Generator : Creating File : " + Arrays.toString(csvText) + ANSI_RESET);

            try {
                auditCSV.flush();
                auditCSV.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // Sleep for a random time ( 1 - 3 secs) before the next record.
            try {
                Thread.sleep(random.nextInt(2000) + 1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
