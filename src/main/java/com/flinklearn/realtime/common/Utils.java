package com.flinklearn.realtime.common;

import org.slf4j.Logger;

public class Utils {

    public static void printHeader(Logger log, String msg) {
        log.info("\n**************************************************************\n{}\n---------------------------------------------------------------", msg);
    }
}
