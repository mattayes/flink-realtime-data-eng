package com.flinklearn.realtime.common;

import org.slf4j.Logger;

public class Utils {

    public static void printHeader(Logger log, String msg) {
        log.info("\n**************************************************************\n{}\n---------------------------------------------------------------", msg);
    }

    public static void printHeader(String msg) {
        System.out.println("\n**************************************************************");
        System.out.println(msg);
        System.out.println("---------------------------------------------------------------\n");
    }
}
