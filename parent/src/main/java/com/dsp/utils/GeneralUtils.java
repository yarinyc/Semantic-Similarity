package com.dsp.utils;

import java.util.Date;


public class GeneralUtils {

    public static boolean DEBUG = false;

    public enum Counters{
        COUNTL,
        COUNTF
    }

    public static void setDebug(boolean val){
        DEBUG = val;
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

    // use the stemmer to create a lexeme for any string
    public static String stem(String str, Stemmer stemmer){
        for (int i = 0; i <str.length() ; i++) {
            stemmer.add(str.charAt(i));
        }
        stemmer.stem();
        return stemmer.toString();
    }


}
