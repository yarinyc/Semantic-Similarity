package com.dsp.utils;

import java.util.Date;


public class GeneralUtils {

    public static boolean DEBUG = false;

    public enum Counters{
        COUNTL,
        COUNTF
    }

    public static void logPrint(String message){
        if(DEBUG){
            String s = String.format("%s - %s", new Date().toString(), message);
            System.err.println(s);
        }
    }

    // parse string coded (comma separated) pairs : VAL1,VAL2
    public static String[] parsePair(String pair){
        String[] res = pair.split(",");
        if(res.length > 2){
            throw new RuntimeException("In parsePair: input was not a pair");
        }
        return res;
    }
}
