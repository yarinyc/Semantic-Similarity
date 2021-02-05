package com.dsp.application;

import com.dsp.commonResources.Pair;
import com.dsp.utils.GeneralUtils;
import com.dsp.utils.Stemmer;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

public class WekaClassifier {

    public static final boolean SHOULD_CREATE_CSV = true;
    public static final String COLUMNS = "word1,word2,label," +
            "freq-manhattan,freq-euclidean,freq-cosine,freq-Jacard,freq-Dice,freq-JS," +
            "prob-manhattan,prob-euclidean,prob-cosine,prob-Jacard,prob-Dice,prob-JS," +
            "PMI-manhattan,PMI-euclidean,PMI-cosine,PMI-Jacard,PMI-Dice,PMI-JS," +
            "t-test-manhattan,t-test-euclidean,t-test-cosine,t-test-Jacard,t-test-Dice,t-test-JS,";

    public static void main(String[] args) {

        String dataCSVFileName = Paths.get("resources", "data.csv").toString();
        String rawDataFileName = Paths.get("resources", "part-r-00000").toString();
        if(SHOULD_CREATE_CSV){
            convertDataToCSV(dataCSVFileName, rawDataFileName);
            GeneralUtils.print("data.csv file was created successfully");
        }


//        CSVLoader loader = new CSVLoader();
//        loader.setFieldSeparator(",");
//        loader.setSource(new File(fileName));
//        Instances instances = loader.getDataSet();
//
//
//        System.out.println(instances);

    }

    private static void convertDataToCSV(String dataCSVFileName, String rawDataFileName) {
        Stemmer stemmer = new Stemmer();
        try {
            List<String[]> goldenStandard = parseGoldenStandard(Paths.get("resources", "word-relatedness.txt").toString());
            File f = new File(dataCSVFileName);
            if(!f.createNewFile()){
                System.out.println("Error: failed to create the data.csv file");
                System.exit(1);
            }
            Files.write(Paths.get(dataCSVFileName), COLUMNS.getBytes(), StandardOpenOption.APPEND);

            List<String> lines = Files.readAllLines(Paths.get(rawDataFileName), StandardCharsets.UTF_8);
            for(String line : lines){
                String[] splitLine = line.split("\t");
                String[] wordPair = splitLine[0].substring(1, splitLine[0].length()-1).split(",");

                Optional<String[]> gsLIneOpt = goldenStandard.stream()
                        .filter(e -> GeneralUtils.stem(e[0], stemmer).equals(wordPair[0]) && GeneralUtils.stem(e[1], stemmer).equals(wordPair[1]))
                        .findAny();

                if(gsLIneOpt.isPresent()){ //Optional object may not be present (should not happen in our file)
                    String[] gsLine = gsLIneOpt.get();
                    addToCSVFile(dataCSVFileName, splitLine[1], gsLine);
                }
                else{
                    System.out.println("Error: no gs line found for word pair: " + wordPair[0] + ", " + wordPair[1] );
                    System.exit(1);
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    // append 1 line from data to the .csv file
    private static void addToCSVFile(String dataCSVFileName, String vectorString, String[] gsLine) throws IOException {
        StringBuilder nextLine = new StringBuilder();
        //append w1,w2,label
        nextLine.append("\n").append(gsLine[0]).append(",").append(gsLine[1]).append(",").append(gsLine[2]);
        // append all vector values
        String[] vector = vectorString.substring(1, vectorString.length()-1).split(", ");
        for (String v : vector){
            switch (v) {
                case "NaN":
                    nextLine.append(",").append("0"); //TODO what to put instead of NaN
                    break;
                case "-Infinity":
                    nextLine.append(",").append(Double.MIN_VALUE);
                    break;
                case "Infinity":
                    nextLine.append(",").append(Double.MAX_VALUE);
                    break;
                default:
                    nextLine.append(",").append(v);
                    break;
            }
        }
        Files.write(Paths.get(dataCSVFileName), nextLine.toString().getBytes(), StandardOpenOption.APPEND);
    }

    public static List<String[]> parseGoldenStandard(String fileName) throws IOException {
        return Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8) //read from gold standard file
                .stream()
                .filter(l -> !l.isEmpty())      // filter empty lines
                .map(l -> l.split("\t"))  // split each line to {w1,w2,label}
                .filter(l -> l.length == 3)    // filter non legal lines
                .collect(Collectors.toList());
    }
}
