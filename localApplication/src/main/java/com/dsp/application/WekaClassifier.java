package com.dsp.application;

import com.dsp.utils.GeneralUtils;
import com.dsp.utils.Stemmer;

import weka.classifiers.Evaluation;
import weka.classifiers.evaluation.ConfusionMatrix;
import weka.classifiers.evaluation.NominalPrediction;
import weka.classifiers.evaluation.Prediction;
import weka.classifiers.trees.RandomForest;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils.DataSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class WekaClassifier {

    public static final String CSV_HEADER = "word1,word2,label," +
            "freq-manhattan,freq-euclidean,freq-cosine,freq-Jacard,freq-Dice,freq-JS," +
            "prob-manhattan,prob-euclidean,prob-cosine,prob-Jacard,prob-Dice,prob-JS," +
            "PMI-manhattan,PMI-euclidean,PMI-cosine,PMI-Jacard,PMI-Dice,PMI-JS," +
            "T-test-manhattan,T-test-euclidean,T-test-cosine,T-test-Jacard,T-test-Dice,T-test-JS";

    public static LocalAppConfiguration config;

    public static void runWeka(LocalAppConfiguration localAppConfiguration) throws Exception {

        config = localAppConfiguration;
        String dataCSVFileName = Paths.get("resources", "data.csv").toString();
        String dataARFFileName = Paths.get("resources", "data.arff").toString();
        String rawDataFileName = Paths.get("resources", "vectors.txt").toString();

        if(!new File(dataARFFileName).exists()){ // create the data files (data.csv & data.arff) only if they don't exist
            // write to resources/vectors our entire data
            getAllVectorData(rawDataFileName);
            prepareData(dataCSVFileName, dataARFFileName, rawDataFileName);
        }
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ load data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        DataSource source = new DataSource(dataARFFileName);
        Instances instances = source.getDataSet();
        // print instances before training the model
        System.out.println(instances.toSummaryString());
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ configure data parameters ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//        instances.randomize(new Random(23)); // randomize order of input records
        instances.deleteAttributeAt(0);
        instances.deleteAttributeAt(0);
        instances.setClassIndex(0); // index of the target variable - 3rd column is the label

        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ classification model ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        RandomForest classifier = new RandomForest();
        classifier.setMaxDepth(20);
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ evaluate model ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        Evaluation evaluation = new Evaluation(instances);
        evaluation.crossValidateModel(classifier, instances, 10, new Random(23)); // 10-fold cross-validation
        System.out.println(evaluation.toSummaryString());
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        // a quick overview of the actual classes from our test data versus the predicted values from our model
        ConfusionMatrix confusionMatrix = new ConfusionMatrix(new String[] {"False", "True"});
        confusionMatrix.addPredictions(evaluation.predictions());


        double tp = confusionMatrix.get(1, 1);
        double fp = confusionMatrix.get(0, 1);
        double tn = confusionMatrix.get(0, 0);
        double fn = confusionMatrix.get(1, 0);

        double precision = tp/(tp+fp);
        double recall = tp/(tp+fn);

        System.out.println(confusionMatrix.toString());
        System.out.println("Accuracy: " + evaluation.pctCorrect());
        //System.out.println("Precision: " + evaluation.precision(1));
        System.out.println("Precision: " + precision);
        //System.out.println("Recall: " + evaluation.recall(1));
        System.out.println("Recall: " + recall);
        System.out.println("F1 score: " + 2 * (precision * recall)/(precision + recall));

        System.out.println();
        CSVLoader loader = new CSVLoader();
        loader.setFieldSeparator(",");
        loader.setSource(new File(dataCSVFileName));
        Instances data = loader.getDataSet();
        ArrayList<Prediction> predictions = evaluation.predictions();
        int x = 0;
        for (int i=0; i<predictions.size(); i++) {
            Prediction p = predictions.get(i);
            Instance instance = data.get(i);
            if(p.predicted() == 1.0 && p.predicted() == p.actual()) {
                System.out.println(instance);
                System.out.println(p.toString());
                System.out.println();
                x++;
            }
        }
        System.out.println(x);
    }

    private static void getAllVectorData(String rawDataFileName) throws IOException {

        String rawDataDir = Paths.get("resources", "rawData").toString();
        String vectorsTxt = Paths.get("resources", "vectors.txt").toString();
        String bucketName = config.getS3BucketName();
        File rawDataDirFile = new File(rawDataDir);
        if(!rawDataDirFile.exists() && !new File(vectorsTxt).exists()) {
            try {
                GeneralUtils.print("Downloading results from s3...");
                Runtime.getRuntime()
                        .exec("aws s3 cp s3://" + bucketName + "/step_8_results/ " + rawDataDir + " --recursive")
                        .waitFor(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        else {
            GeneralUtils.print("vectors.txt data file exists");
        }

        File vectorsFolder = new File(rawDataDir);
        File[] files = vectorsFolder.listFiles();
        File outFile = new File(rawDataFileName);
        if(!outFile.createNewFile())
            return;
        if (files != null) {
            for (File f : files) {
                if (f.getName().equals("_SUCCESS")){ // skip this file
                    continue;
                }
                List<String> lines = Files.readAllLines(Paths.get(f.getPath()), StandardCharsets.UTF_8).stream().filter(l -> !l.isEmpty()).collect(Collectors.toList());
                for (String line : lines) {
                    Files.write(Paths.get(rawDataFileName), (line+"\n").getBytes(), StandardOpenOption.APPEND);
                }
            }
        }
    }

    // convert data from the map reduce flow to csv format and then convert it into .arff format for Weka
    private static void prepareData(String dataCSVFileName, String dataARFFileName, String rawDataFileName) throws IOException {

        GeneralUtils.print("calculating mean values for all columns...");
        double[] means = calculateMeans(rawDataFileName);

        GeneralUtils.print("creating data.csv file...");
        convertDataToCSV(dataCSVFileName, rawDataFileName, means);
        GeneralUtils.print("data.csv file was created successfully");
        CSVLoader loader = new CSVLoader();
        loader.setFieldSeparator(",");
        loader.setSource(new File(dataCSVFileName));
        Instances data = loader.getDataSet();

        GeneralUtils.print("converting data.csv to .arff format...");
        ArffSaver saver = new ArffSaver();
        saver.setInstances(data);
        saver.setFile(new File(dataARFFileName));
        saver.writeBatch();
        GeneralUtils.print("data.arff file was created successfully");
    }

    // calculate the mean (in our case - average) of each column in our data
    private static double[] calculateMeans(String rawDataFileName) throws IOException {
        List<String[]> lines = Files.readAllLines(Paths.get(rawDataFileName), StandardCharsets.UTF_8).stream()
                .filter(l->!l.isEmpty())
                .map(l-> {
                    String v = l.split("\t")[1];
                    return v.substring(1, v.length()-1).split(", ");
                })
                .collect(Collectors.toList());
        double[] means = new double[24];
        int[] counters = new int[24];
        for (String[] l : lines){
            for (int i=1; i<l.length; i++){
                double number = Double.parseDouble(l[i]);
                if(!Double.isNaN(number) && Double.isFinite(number)){
                    means[i-1] += Double.parseDouble(l[i]);
                    counters[i-1]++;
                }
            }
        }
        for (int i=0; i<24; i++){
            means[i] /= counters[i];
        }
        return means;
    }

    private static void convertDataToCSV(String dataCSVFileName, String rawDataFileName, double[] means) {
        Stemmer stemmer = new Stemmer();
        try {
            List<String[]> goldenStandard = parseGoldenStandard(Paths.get("resources", "word-relatedness.txt").toString());
            File f = new File(dataCSVFileName);
            if(!f.createNewFile()){
                System.out.println("Error: failed to create the data.csv file");
                System.exit(1);
            }
            Files.write(Paths.get(dataCSVFileName), CSV_HEADER.getBytes(), StandardOpenOption.APPEND);

            List<String> lines = Files.readAllLines(Paths.get(rawDataFileName), StandardCharsets.UTF_8);
            for(String line : lines){
                String[] splitLine = line.split("\t");
                String[] wordPair = splitLine[0].substring(1, splitLine[0].length()-1).split(",");

                Optional<String[]> gsLIneOpt = goldenStandard.stream()
                        .filter(e -> GeneralUtils.stem(e[0], stemmer).equals(wordPair[0]) && GeneralUtils.stem(e[1], stemmer).equals(wordPair[1]))
                        .findAny();

                if(gsLIneOpt.isPresent()){ //Optional object may not be present (should not happen in our file)
                    String[] gsLine = gsLIneOpt.get();
                    addToCSVFile(dataCSVFileName, splitLine[1], gsLine, means);
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
    // in the case that a value in the vector is NaN or +-Infinity we replace that value with the column's mean value
    private static void addToCSVFile(String dataCSVFileName, String vectorString, String[] gsLine, double[] means) throws IOException {
        StringBuilder nextLine = new StringBuilder();
        //append w1,w2,label
        nextLine.append("\n").append(gsLine[0]).append(",").append(gsLine[1]).append(",").append(gsLine[2]);
        // append all vector values
        String[] vector = vectorString.substring(1, vectorString.length()-1).split(", ");
        for (int i=0; i<vector.length; i++){
            String v = vector[i];
            switch (v) {
                case "NaN":
                case "Infinity":
                case "-Infinity":
                    nextLine.append(",").append(means[i]);
                    break;
                default:
                    nextLine.append(",").append(v);
                    break;
            }
        }
        Files.write(Paths.get(dataCSVFileName), nextLine.toString().getBytes(), StandardOpenOption.APPEND);
    }

    // return a list of all lines in the GS, each line is represented by an array of size 3
    public static List<String[]> parseGoldenStandard(String fileName) throws IOException {
        return Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8) //read from gold standard file
                .stream()
                .filter(l -> !l.isEmpty())      // filter empty lines
                .map(l -> l.split("\t"))  // split each line to {w1,w2,label}
                .filter(l -> l.length == 3)    // filter non legal lines
                .collect(Collectors.toList());
    }
}
