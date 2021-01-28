package com.dsp.step2CountLexemesFeatures;

import com.dsp.commonResources.Biarc;
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

public class Step2CountLexemesFeatures {

    public static class MapperClass extends Mapper<Text, Biarc, Text, LongWritable> {

        // for each biarc emit all its features + total count
        @Override
        public void map(Text key, Biarc value, Context context) throws IOException,  InterruptedException {
            for (String feature : value.getFeatures()) {
                Text outKey = new Text(value.getRootLexeme().toString() + "," + feature);
                GeneralUtils.logPrint("In step2 map: <lexeme,feature> = " + outKey.toString() + " count = " + value.getTotalCount().get());
                context.write(outKey, value.getTotalCount());
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {

        // for each <lexeme,feature>, sum all counts (calculate count(F=f,L=l))
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            GeneralUtils.logPrint("In step2 reduce: <lexeme,feature> = " + key.toString() + " count = " + sum);
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
        boolean debug = Boolean.parseBoolean(args[4]);
        GeneralUtils.setDebug(debug);

        Configuration conf = new Configuration();

        Job job = new Job(conf, "countLexemesFeatures");
        job.setJarByClass(Step2CountLexemesFeatures.class);
        job.setMapperClass(Step2CountLexemesFeatures.MapperClass.class);
        job.setCombinerClass(Step2CountLexemesFeatures.ReducerClass.class);
        job.setPartitionerClass(Step2CountLexemesFeatures.PartitionerClass.class);
        job.setReducerClass(Step2CountLexemesFeatures.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        Path inputPath = new Path(s3BucketUrl+input);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output)); // "step_2_results/"

        boolean isDone = job.waitForCompletion(true);

        System.exit(isDone ? 0 : 1);
    }
}
