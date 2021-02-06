package com.dsp.application;

import com.dsp.utils.GeneralUtils;
import com.dsp.utils.Stemmer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.evaluation.ConfusionMatrix;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils.DataSource;

public class WekaClassifier {

    public static final String CSV_HEADER = "word1,word2,label," +
            "freq-manhattan,freq-euclidean,freq-cosine,freq-Jacard,freq-Dice,freq-JS," +
            "prob-manhattan,prob-euclidean,prob-cosine,prob-Jacard,prob-Dice,prob-JS," +
            "PMI-manhattan,PMI-euclidean,PMI-cosine,PMI-Jacard,PMI-Dice,PMI-JS," +
            "T-test-manhattan,T-test-euclidean,T-test-cosine,T-test-Jacard,T-test-Dice,T-test-JS";

    public static void main(String[] args) throws Exception {

        String dataCSVFileName = Paths.get("resources", "data.csv").toString();
        String dataARFFileName = Paths.get("resources", "data.arff").toString();
        String rawDataFileName = Paths.get("resources", "part-r-00000").toString();

        // TODO maybe add function for downloading the data from s3 to ./resources/ directory

        if(!new File(dataARFFileName).exists()){ // create the data files (data.csv & data.arff) only if they don't exist
            prepareData(dataCSVFileName, dataARFFileName, rawDataFileName);
        }

        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ load data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        DataSource source = new DataSource(dataARFFileName);
        Instances instances = source.getDataSet();

        // print instances before training the model
        System.out.println(instances.toSummaryString());

        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ configure data parameters ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        // TODO: probably target variable is the label: true/false ||| do we need this? what is the purpose of a class index
        instances.setClassIndex(2); // index of the target variable - 3rd column is the label
        instances.randomize(new Random(23)); // randomize order of input records

        // assign training size and testing size for the classifier
        int trainSize = (int) Math.round(instances.numInstances() * 0.9); // take 90% as training data
        int testSize = instances.numInstances() - trainSize;

        // instantiate training & test instances
        Instances trainInstances = new Instances(instances, 0, trainSize);
        Instances testInstances = new Instances(instances, trainSize, testSize);

        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ classification model ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//        NaiveBayes classifier = new NaiveBayes();
        MultilayerPerceptron classifier = new MultilayerPerceptron(); // TODO takes a long time or doesn't work
        classifier.setLearningRate(0.1);
        classifier.setMomentum(0.2);
        classifier.setTrainingTime(100);
        classifier.setHiddenLayers("3");

//        classifier.buildClassifier(trainInstances); // train model

        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ evaluate model ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        // use test data to evaluate model
        Evaluation evaluation = new Evaluation(instances);
//        evaluation.evaluateModel(classifier, testInstances); // normal evaluation
        evaluation.crossValidateModel(classifier, instances, 10, new Random(23)); // 10-fold cross-validation TODO make sure this is correct
        System.out.println(evaluation.toSummaryString());

        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

        // a quick overview of the actual classes from our test data versus the predicted values from our model
        ConfusionMatrix confusionMatrix = new ConfusionMatrix(new String[] {"False", "True"});
        confusionMatrix.addPredictions(evaluation.predictions());

        System.out.println(confusionMatrix.toString());

        System.out.println("Accuracy: " + evaluation.pctCorrect());
        System.out.println("Precision: " + evaluation.precision(1)); // TODO check what is this class index
        System.out.println("Recall: " + evaluation.recall(1));

    }

    // convert data from the map reduce flow to csv format and then convert it into .arff format for Weka
    private static void prepareData(String dataCSVFileName, String dataARFFileName, String rawDataFileName) throws IOException {
        convertDataToCSV(dataCSVFileName, rawDataFileName);
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

    private static void convertDataToCSV(String dataCSVFileName, String rawDataFileName) {
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
