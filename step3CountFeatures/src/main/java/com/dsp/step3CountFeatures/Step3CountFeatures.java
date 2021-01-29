package com.dsp.step3CountFeatures;

import com.dsp.utils.GeneralUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Arrays;

public class Step3CountFeatures {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, LongWritable> {
        // for each <lexeme,feature> pair, emit feature with it's count
        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException,  InterruptedException {
            String[] lexemeFeaturePair = GeneralUtils.parsePair(key.toString());
            GeneralUtils.logPrint("In step3 map: feature = " + lexemeFeaturePair[1] + " count = " + value);
            context.write(new Text(lexemeFeaturePair[1]), value);
        }
    }

    public static class CombinerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        // for each feature calculate sum ( calculates count(F=f) & count(F) )
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            GeneralUtils.logPrint("In step3 combiner: feature = " + key.toString() + " count = " + sum);
            context.write(key, new LongWritable(sum));
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        // for each feature calculate sum & increase COUNTF counter by 1 ( calculates count(F=f) & count(F) )
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.getCounter(GeneralUtils.Counters.COUNTF).increment(1);
            GeneralUtils.logPrint("In step3 reduce: feature = " + key.toString() + " count = " + sum);
            context.write(key, new LongWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return Math.abs(key.toString().hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String s3BucketName = args[1];
        String s3BucketUrl = String.format("s3://%s/", s3BucketName);
        String input = args[2];
        String output = args[3];

        // set debug flag for logging
        System.err.println(args[4]);
        boolean debug = Boolean.parseBoolean(args[4]);
        GeneralUtils.setDebug(debug);

        Configuration conf = new Configuration();

        Job job = new Job(conf, "step3CountFeatures");
        job.setJarByClass(Step3CountFeatures.class);
        job.setMapperClass(Step3CountFeatures.MapperClass.class);
        job.setCombinerClass(Step3CountFeatures.CombinerClass.class);
        job.setPartitionerClass(Step3CountFeatures.PartitionerClass.class);
        job.setReducerClass(Step3CountFeatures.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path inputPath = new Path(s3BucketUrl+input);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output));

        boolean isDone = job.waitForCompletion(true);

        FileSystem fileSystem = FileSystem.get(URI.create("s3://" + s3BucketName), conf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("s3://" + s3BucketName +"/COUNTF"));

        PrintWriter writer  = new PrintWriter(fsDataOutputStream);
        String countF = Long.toString(job.getCounters().findCounter(GeneralUtils.Counters.COUNTF).getValue());
        writer.write(countF);
        GeneralUtils.logPrint("in end of step3: COUNTF="+countF);

        writer.close();
        fsDataOutputStream.close();

        System.exit(isDone ? 0 : 1);
    }
}