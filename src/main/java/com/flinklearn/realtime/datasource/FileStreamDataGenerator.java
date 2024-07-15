package com.flinklearn.realtime.datasource;

import com.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;

import java.io.File;
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

public class FileStreamDataGenerator implements Runnable {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_BLUE = "\u001B[34m";
    //Define the data directory to output the files
    private static final String dataDir = "data/raw_audit_trail";

    public static void main(String[] args) throws Exception {
        FileUtils.cleanDirectory(new File(dataDir));
        FileStreamDataGenerator fsdg = new FileStreamDataGenerator();
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
        appOperation.add("Create");
        appOperation.add("Modify");
        appOperation.add("Query");
        appOperation.add("Delete");

        //Define list of application entities
        final List<String> appEntity = new ArrayList<>();
        appEntity.add("Customer");
        appEntity.add("SalesRep");

        //Define a random number generator
        final Random random = new Random();

        //Generate 100 sample audit records, one per each file
        for (int i = 0; i < 100; i++) {

            //Capture current timestamp
            final String currentTime = String.valueOf(System.currentTimeMillis());

            //Generate a random user
            final String user = appUser.get(random.nextInt(appUser.size()));
            //Generate a random operation
            final String operation = appOperation.get(random.nextInt(appOperation.size()));
            //Generate a random entity
            final String entity = appEntity.get(random.nextInt(appEntity.size()));
            //Generate a random duration for the operation
            final String duration = String.valueOf(random.nextInt(10) + 1);
            //Generate a random value for number of changes
            final String changeCount = String.valueOf(random.nextInt(4) + 1);

            //Create a CSV Text array
            final String[] csvText = {String.valueOf(i), user, entity,
                    operation, currentTime, duration, changeCount};

            //Open a new file for this record
            FileWriter auditFile;
            try {
                auditFile = new FileWriter(dataDir
                        + "/audit_trail_" + i + ".csv");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            final CSVWriter auditCSV = new CSVWriter(auditFile);

            //Write the audit record and close the file
            auditCSV.writeNext(csvText);

            System.out.println(ANSI_BLUE + "FileStream Generator : Creating File : "
                    + Arrays.toString(csvText) + ANSI_RESET);

            try {
                auditCSV.flush();
                auditCSV.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


            //Sleep for a random time ( 1 - 3 secs) before the next record.
            try {
                Thread.sleep(random.nextInt(2000) + 1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }


}
