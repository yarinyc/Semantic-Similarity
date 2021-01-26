package com.dsp.utils;

import com.dsp.commonResources.LongPair;
import com.dsp.commonResources.Biarc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.Date;


public class GeneralUtils {

    public static boolean DEBUG = false;

    public enum Counters{
        N
    }

    public static void logPrint(String message){
        if(DEBUG){
            String s = String.format("%s - %s", new Date().toString(), message);
            System.err.println(s);
        }
    }
}
