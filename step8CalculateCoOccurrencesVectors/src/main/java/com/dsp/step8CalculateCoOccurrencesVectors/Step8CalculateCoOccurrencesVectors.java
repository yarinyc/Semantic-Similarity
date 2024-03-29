package com.dsp.step8CalculateCoOccurrencesVectors;

import com.dsp.commonResources.Pair;
import com.dsp.commonResources.SimilarityCalculator;
import com.dsp.utils.GeneralUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class Step8CalculateCoOccurrencesVectors {

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        public static Map<Pair<String, String>, Boolean> gsMap;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            boolean debug = Boolean.parseBoolean(context.getConfiguration().get("DEBUG"));
            GeneralUtils.setDebug(debug);

            //read golden dataset
            String bucketName = context.getConfiguration().get("bucketName");
            FileSystem fileSystem = FileSystem.get(URI.create("s3://" + bucketName), context.getConfiguration());
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path("s3://" + bucketName + "/input/word-relatedness.txt"));
            gsMap = GeneralUtils.parseGoldenStandard(fsDataInputStream);
            fsDataInputStream.close();
            fileSystem.close();

        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException,  InterruptedException {
            // wordPairs is a list containing all word pairs in GS dataset that contain the key lexeme
            List<Pair<String,String>> wordPairs = GeneralUtils.getWordPairs(gsMap,key.toString());
            GeneralUtils.logPrint("In step8 map: key is " + key.toString() + " , wordPairs are " + wordPairs);
            // newValue = <key lexeme, value(feature list of lexeme)>
            Text newValue = new Text(key.toString() + "\t\t" + value.toString());
            for(Pair wordPair : wordPairs){
                Text newKey = new Text(wordPair.toString());
                // emit key = wordPair (<l,l'> of <l',l> where l is the key lexeme), value = <key lexeme, feature list of key lexeme>
                context.write(newKey,newValue);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text,Text, Text> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            boolean debug = Boolean.parseBoolean(context.getConfiguration().get("DEBUG"));
            GeneralUtils.setDebug(debug);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            HashMap<String,Double> firstAssocFreq = new HashMap<>();
            HashMap<String,Double> firstAssocProb = new HashMap<>();
            HashMap<String,Double> firstAssocPMI = new HashMap<>();
            HashMap<String,Double> firstAssocT = new HashMap<>();
            HashMap<String,Double> secondAssocFreq = new HashMap<>();
            HashMap<String,Double> secondAssocProb = new HashMap<>();
            HashMap<String,Double> secondAssocPMI = new HashMap<>();
            HashMap<String,Double> secondAssocT = new HashMap<>();

            String[] splitKey = key.toString().substring(1,key.toString().length()-1).split(",");
            String firstWord = splitKey[0];
            String secondWord = splitKey[1];
            GeneralUtils.logPrint("In reduce step 8: l1 = "+ firstWord + " l2 = " + secondWord);

            // values should be of size 2: all features of first word + all features of second word
            for(Text value : values){
                List<String> splitValue = Arrays.stream(value.toString().split("\t\t")).filter(x -> !x.isEmpty()).collect(Collectors.toList());
                String lexeme = splitValue.get(0);
                // for each feature in value, assign assoc values of feature to hashmaps, according to the lexeme (l or l' of the key <l,l'> or <l',l>)
                for(int i=1; i<splitValue.size(); i++) {
                    String[] featureAssocs = splitValue.get(i).split("\t");
                    String feature = featureAssocs[0];
                    String[] assocValues = featureAssocs[1].substring(1, featureAssocs[1].length() - 1).split(", ");
                    // if l is the first word in the key
                    if (lexeme.equals(firstWord)) {
                        firstAssocFreq.put(feature,Double.parseDouble(assocValues[0]));
                        firstAssocProb.put(feature,Double.parseDouble(assocValues[1]));
                        firstAssocPMI.put(feature,Double.parseDouble(assocValues[2]));
                        firstAssocT.put(feature,Double.parseDouble(assocValues[3]));
                    }
                    // if l is the second word in the key
                    else if (lexeme.equals(secondWord)) {
                        secondAssocFreq.put(feature,Double.parseDouble(assocValues[0]));
                        secondAssocProb.put(feature,Double.parseDouble(assocValues[1]));
                        secondAssocPMI.put(feature,Double.parseDouble(assocValues[2]));
                        secondAssocT.put(feature,Double.parseDouble(assocValues[3]));
                    }
                    // this should not happen
                    else {
                        GeneralUtils.logPrint("In reduce step 8 ERROR: key is " + key.toString() + " and lexeme is: " + lexeme);
                    }
                }
            }

            // compute 24-d co-occurrence vector
            List<Double> coOccurrenceVector = new ArrayList<>();

            // for each of the 4 association with context vector calculate similarities (vectors represented as hashmaps)
            //sim1:
            SimilarityCalculator simCalc1 = new SimilarityCalculator(firstAssocFreq , secondAssocFreq);
            List<Double> similarityScores1 = simCalc1.getAllSimilarities();
            GeneralUtils.logPrint("In step8 reduce: key = " + key.toString() + " similarityScores1 are - " + similarityScores1.toString());
            coOccurrenceVector.addAll(similarityScores1);

            //sim2:
            SimilarityCalculator simCalc2 = new SimilarityCalculator(firstAssocProb , secondAssocProb);
            List<Double> similarityScores2 = simCalc2.getAllSimilarities();
            GeneralUtils.logPrint("In step8 reduce: key = " + key.toString() + " similarityScores2 are - " + similarityScores2.toString());
            coOccurrenceVector.addAll(similarityScores2);

            //sim3:
            SimilarityCalculator simCalc3 = new SimilarityCalculator(firstAssocPMI , secondAssocPMI);
            List<Double> similarityScores3 = simCalc3.getAllSimilarities();
            GeneralUtils.logPrint("In step8 reduce: key = " + key.toString() + " similarityScores3 are - " + similarityScores3.toString());
            coOccurrenceVector.addAll(similarityScores3);

            //sim4:
            SimilarityCalculator simCalc4 = new SimilarityCalculator(firstAssocT , secondAssocT);
            List<Double> similarityScores4 = simCalc4.getAllSimilarities();
            GeneralUtils.logPrint("In step8 reduce: key = " + key.toString() + " similarityScores4 are - " + similarityScores4.toString());
            coOccurrenceVector.addAll(similarityScores4);

            GeneralUtils.logPrint("In step8 reduce: final co-occurrence vector for wordPair: " + key.toString() + " is - " + coOccurrenceVector.toString());

            // emit key = wordPair (<l,l'>), value = 24-d coOccurrence vector
            context.write(key, new Text(coOccurrenceVector.toString()));

        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.toString().hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String s3BucketName = args[1];
        String s3BucketUrl = String.format("s3://%s/", s3BucketName);
        String input = args[2];
        String output = args[3];

        // set debug flag for logging
        boolean debug = Boolean.parseBoolean(args[4]);
        GeneralUtils.setDebug(debug);

        Configuration conf = new Configuration();
        conf.set("DEBUG", Boolean.toString(debug));
        conf.set("bucketName", s3BucketName);

        Job job = new Job(conf, "step8CalculateCoOccurrencesVectors");
        job.setJarByClass(Step8CalculateCoOccurrencesVectors.class);
        job.setMapperClass(Step8CalculateCoOccurrencesVectors.MapperClass.class);
        job.setPartitionerClass(Step8CalculateCoOccurrencesVectors.PartitionerClass.class);
        job.setReducerClass(Step8CalculateCoOccurrencesVectors.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path inputPath = new Path(s3BucketUrl+input);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output));

        boolean isDone = job.waitForCompletion(true);

        System.exit(isDone ? 0 : 1);
    }
}
