package com.dsp.utils;

import com.dsp.commonResources.Pair;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class GeneralUtils {

    public static boolean DEBUG = true;

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
        if(res.length != 2){
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

    // parse GS file
    public static Map<Pair<String,String>, Boolean> parseGoldenStandard(FSDataInputStream fsDataInputStream){
        List<String> lines;
        try {
            lines = IOUtils.readLines(fsDataInputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logPrint(Arrays.toString(e.getStackTrace()));
            return null;
        }
        Map<Pair<String,String>, Boolean> gsMap = new HashMap<>();
        for (String line : lines){
            String[] gsLine = line.split("\t");
            if(line.equals("") || gsLine.length != 3) { // each line in GS should have 3 string: word<TAB>word<TAB>true/false
                continue;
            }
            Pair<String,String> gsPair = Pair.of(gsLine[0], gsLine[1]); // pair of 2 golden standard words
            gsMap.put(gsPair, Boolean.parseBoolean(gsLine[2]));
        }
        return gsMap;
    }

    // returns list of pairs in gsMap that contain a specific given lexeme
    public static List<Pair<String,String>> getWordPairs(Map<Pair<String,String>, Boolean> map, String lexeme){
        List<Pair<String,String>> result = map.keySet()
                .stream()
                .filter(x -> x.getFirst().equals(lexeme) || x.getSecond().equals(lexeme))
                .collect(Collectors.toList());

        return result;
    }
}
