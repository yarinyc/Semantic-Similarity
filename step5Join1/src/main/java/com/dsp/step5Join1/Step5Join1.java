package com.dsp.step5Join1;

import com.dsp.commonResources.LongPair;
import com.dsp.utils.GeneralUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class Step5Join1 {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongPair> {

        @Override
        public void map(LongWritable rowID, Text line, Context context) throws IOException,  InterruptedException {

        }
    }

    public static class ReducerClass extends Reducer<Text, LongPair,Text, LongPair> {

        @Override
        public void reduce(Text key, Iterable<LongPair> values, Context context) throws IOException,  InterruptedException {

        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongPair> {
        @Override
        public int getPartition(Text key, LongPair value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String s3BucketName = args[1];
        String s3BucketUrl = String.format("s3://%s/", s3BucketName);
        String input = args[2]; // TODO separate \t inputs
        String output = args[3];

        // set debug flag for logging
        boolean debug = Boolean.parseBoolean(args[4]);
        GeneralUtils.setDebug(debug);

        Configuration conf = new Configuration();

        Job job = new Job(conf, "step5Join1");
        job.setJarByClass(Step5Join1.class);
        job.setMapperClass(Step5Join1.MapperClass.class);
        job.setCombinerClass(Step5Join1.ReducerClass.class);
        job.setPartitionerClass(Step5Join1.PartitionerClass.class);
        job.setReducerClass(Step5Join1.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongPair.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        Path inputPath = new Path(s3BucketUrl+input);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output));

        boolean isDone = job.waitForCompletion(true);

        System.exit(isDone ? 0 : 1);
    }
}
