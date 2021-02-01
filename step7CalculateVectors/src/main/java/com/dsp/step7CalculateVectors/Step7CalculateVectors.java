package com.dsp.step7CalculateVectors;

import com.dsp.utils.GeneralUtils;
import org.apache.hadoop.conf.Configuration;
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

public class Step7CalculateVectors {


    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            boolean debug = Boolean.parseBoolean(context.getConfiguration().get("DEBUG"));
            GeneralUtils.setDebug(debug);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException,  InterruptedException {

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

        Job job = new Job(conf, "step7CalculateVectors");
        job.setJarByClass(Step7CalculateVectors.class);
        job.setMapperClass(Step7CalculateVectors.MapperClass.class);
        job.setCombinerClass(Step7CalculateVectors.ReducerClass.class);
        job.setPartitionerClass(Step7CalculateVectors.PartitionerClass.class);
        job.setReducerClass(Step7CalculateVectors.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path inputPath = new Path(s3BucketUrl+input);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output));

        boolean isDone = job.waitForCompletion(true);

//        FileSystem fileSystem = FileSystem.get(URI.create("s3://" + s3BucketName), conf);
//        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("s3://" + s3BucketName +"/N"));
//
//        PrintWriter writer  = new PrintWriter(fsDataOutputStream);
//        String nValue = Long.toString(job.getCounters().findCounter(GeneralUtils.Counters.N).getValue());
//        writer.write(nValue);
//        GeneralUtils.logPrint("in end of step1: N="+nValue);
//
//        writer.close();
//        fsDataOutputStream.close();

        System.exit(isDone ? 0 : 1);
    }
}
