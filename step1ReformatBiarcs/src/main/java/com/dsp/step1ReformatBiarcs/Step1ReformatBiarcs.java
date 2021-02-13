package com.dsp.step1ReformatBiarcs;

import com.dsp.commonResources.Biarc;
import com.dsp.utils.GeneralUtils;
import com.dsp.utils.Stemmer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.List;

public class Step1ReformatBiarcs {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Biarc> {

        public Stemmer stemmer = new Stemmer();

        // auxiliary function to make sure we biarcs with legal dependencies only
        public boolean checkBiarcValidity(List<String> dependencyIndices, String biarcWords) {
            if(dependencyIndices.size() != biarcWords.split("\t").length){
                return false;
            }
            for(String s : dependencyIndices){
                if(s.isEmpty()){
                    return false;
                }
                else{
                    for(char c :  s.toCharArray()){
                        if(c < 48 || c > 57){ // if dependency is not a number
                            return false;
                        }
                    }
                    if(s.toCharArray().length > 9){
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            boolean debug = Boolean.parseBoolean(context.getConfiguration().get("DEBUG"));
            GeneralUtils.setDebug(debug);
        }

        // parse each line of the data into a Biarc object
        // each Biarc object holds the root lexeme, list of features & total count
        // emit key=rootLexeme, value = Biarc object
        @Override
        public void map(LongWritable lineID, Text line, Context context) throws IOException,  InterruptedException {
            Biarc inputBiarc = Biarc.parseBiarc(line.toString() ,stemmer);
            GeneralUtils.logPrint("In step1 map: line id = " + lineID + " input line = " + line.toString());
            if(checkBiarcValidity(inputBiarc.getDependencies(), inputBiarc.getBiarcWords().toString())) { //check biarc dependencies are valid
                context.write(inputBiarc.getRootLexeme(), inputBiarc);
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Biarc, Text, Biarc> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            boolean debug = Boolean.parseBoolean(context.getConfiguration().get("DEBUG"));
            GeneralUtils.setDebug(debug);
        }

        // no logic in reducer just emit the same
        @Override
        public void reduce(Text key, Iterable<Biarc> values, Context context) throws IOException,  InterruptedException {
            for(Biarc b : values){
                context.write(key, b);
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Biarc> {
        @Override
        public int getPartition(Text key, Biarc value, int numPartitions) {
            return Math.abs(key.toString().hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String s3BucketName = args[1];
        String s3BucketUrl = String.format("s3://%s/", s3BucketName);
        String input = args[2];
        String[] inputFiles = input.split(",");
        String output = args[3];

        // set debug flag for logging
        boolean debug = Boolean.parseBoolean(args[4]);
        GeneralUtils.setDebug(debug);

        Configuration conf = new Configuration();
        conf.set("DEBUG", Boolean.toString(debug));

        Job job = new Job(conf, "reformatBiarcs");
        job.setJarByClass(Step1ReformatBiarcs.class);
        job.setMapperClass(Step1ReformatBiarcs.MapperClass.class);
        job.setPartitionerClass(Step1ReformatBiarcs.PartitionerClass.class);
        job.setReducerClass(Step1ReformatBiarcs.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Biarc.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Biarc.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        for(String in : inputFiles) {
            MultipleInputs.addInputPath(job, new Path(in), SequenceFileInputFormat.class, Step1ReformatBiarcs.MapperClass.class);
        }


        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output));

        boolean isDone = job.waitForCompletion(true);

        System.exit(isDone ? 0 : 1);
    }
}
