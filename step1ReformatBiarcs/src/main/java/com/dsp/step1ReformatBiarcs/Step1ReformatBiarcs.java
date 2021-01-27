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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class Step1ReformatBiarcs {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Biarc> {

        public Stemmer stemmer = new Stemmer();

        // parse each line of the data into a Biarc object
        // each Biarc object holds the root lexeme, list of features & total count
        @Override
        public void map(LongWritable lineID, Text line, Context context) throws IOException,  InterruptedException {
            Biarc inputBiarc = Biarc.parseBiarc(line.toString() ,stemmer);
            GeneralUtils.logPrint("In step1 map: lexeme " + lineID + " Biarc = " + inputBiarc.toString());
            context.write(inputBiarc.getRootLexeme(), inputBiarc);
        }
    }

    public static class ReducerClass extends Reducer<Text, Biarc, Text, Biarc> {
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
        String output = args[3];

        // set debug flag for logging
        boolean debug = Boolean.parseBoolean(args[4]);
        GeneralUtils.setDebug(debug);

        Configuration conf = new Configuration();
        conf.set("bucketName", s3BucketName);

        Job job = new Job(conf, "reformatBiarcs");
        job.setJarByClass(Step1ReformatBiarcs.class);
        job.setMapperClass(Step1ReformatBiarcs.MapperClass.class);
        job.setPartitionerClass(Step1ReformatBiarcs.PartitionerClass.class);
        job.setReducerClass(Step1ReformatBiarcs.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Biarc.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Biarc.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        Path inputPath = new Path(input);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output)); // "step_1_results/"

        boolean isDone = job.waitForCompletion(true);

        System.exit(isDone ? 0 : 1);
    }
}
