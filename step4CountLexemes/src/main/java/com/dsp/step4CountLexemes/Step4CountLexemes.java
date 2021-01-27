package com.dsp.step4CountLexemes;

import com.dsp.commonResources.Biarc;
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

public class Step4CountLexemes {

    public static class MapperClass extends Mapper<Text, Biarc, Text, LongWritable> {
        // for each lexeme  emit lexeme with it's count
        @Override
        public void map(Text key, Biarc value, Context context) throws IOException,  InterruptedException {
            GeneralUtils.logPrint("In step4 map: lexeme = " + key.toString() + " count = " + value.getTotalCount().get());
            context.write(key, value.getTotalCount());
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {
        // for each lexeme calculate sum & increase COUNTL counter by 1 ( calculates count(F=f) & count(F) )
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.getCounter(GeneralUtils.Counters.COUNTL).increment(1);
            GeneralUtils.logPrint("In step4 reduce: lexeme = " + key.toString() + " count = " + sum);
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

        Job job = new Job(conf, "step4CountLexemes");
        job.setJarByClass(Step4CountLexemes.class);
        job.setMapperClass(Step4CountLexemes.MapperClass.class);
        job.setCombinerClass(Step4CountLexemes.ReducerClass.class);
        job.setPartitionerClass(Step4CountLexemes.PartitionerClass.class);
        job.setReducerClass(Step4CountLexemes.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        Path inputPath = new Path(input);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output));

        boolean isDone = job.waitForCompletion(true);

        FileSystem fileSystem = FileSystem.get(URI.create("s3://" + s3BucketName), conf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("s3://" + s3BucketName +"/COUNTL"));

        PrintWriter writer  = new PrintWriter(fsDataOutputStream);
        String countL = Long.toString(job.getCounters().findCounter(GeneralUtils.Counters.COUNTL).getValue());
        writer.write(countL);
        GeneralUtils.logPrint("in end of step4: COUNTL="+countL);

        writer.close();
        fsDataOutputStream.close();

        System.exit(isDone ? 0 : 1);
    }
}
