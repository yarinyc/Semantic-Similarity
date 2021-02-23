package com.dsp.step2CountLexemesFeatures;

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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Step2CountLexemesFeatures {

    public static class MapperClass extends Mapper<Text, Biarc, Text, LongWritable> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            boolean debug = Boolean.parseBoolean(context.getConfiguration().get("DEBUG"));
            GeneralUtils.setDebug(debug);
        }

        // For each biarc and for each lexeme in it, emit all its features + total count
        @Override
        public void map(Text key, Biarc value, Context context) throws IOException,  InterruptedException {

            String[] biarcWords = value.getBiarcWords().toString().split("\t");
            List<String> dependencies = value.getDependencies();
            List<String> features = value.getFeatures();
            LongWritable count = value.getTotalCount();

            // for each word in biarc + all it's features in the biarc, emit the biarc count
            // key = <lexeme,feature> (<stemmed word in biarc,feature>), value = count
            for(int i=0; i<biarcWords.length; i++){
                int dependencyIndex = Integer.parseInt(dependencies.get(i)) - 1;
                GeneralUtils.logPrint("biarcWords =  " + Arrays.toString(biarcWords) + "\tdependencies = " + dependencies.toString() + "\tdependency Index = " + dependencyIndex);
                if((dependencyIndex != -1) && (dependencyIndex < biarcWords.length)) {
                    String lexeme = biarcWords[dependencyIndex];
                    String feature = features.get(i);

                    Text outKey = new Text(lexeme + "," + feature);
                    GeneralUtils.logPrint("In step2 map: <lexeme,feature> = " + outKey.toString() + " count = " + value.getTotalCount().get());
                    context.write(outKey, count);
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            boolean debug = Boolean.parseBoolean(context.getConfiguration().get("DEBUG"));
            GeneralUtils.setDebug(debug);
        }

        // for each <lexeme,feature>, sum all counts (calculate count(F=f,L=l))
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            String lexeme = key.toString().split(",")[0];
            Stemmer stem = new Stemmer();

            if(lexeme.equals(GeneralUtils.stem("glider",stem)) || lexeme.equals(GeneralUtils.stem("animation",stem))|| lexeme.equals(GeneralUtils.stem("bomb",stem))|| lexeme.equals(GeneralUtils.stem("limitation",stem))||lexeme.equals(GeneralUtils.stem("mug",stem))
            ||lexeme.equals(GeneralUtils.stem("co-author",stem))||lexeme.equals(GeneralUtils.stem("bull",stem))||lexeme.equals(GeneralUtils.stem("mammal",stem))||lexeme.equals(GeneralUtils.stem("bottle",stem))||lexeme.equals(GeneralUtils.stem("container",stem))||lexeme.equals(GeneralUtils.stem("cannon",stem))||lexeme.equals(GeneralUtils.stem("weapon",stem))) {
                System.err.println(key.toString());
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
        conf.set("DEBUG", Boolean.toString(debug));

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
        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output));

        boolean isDone = job.waitForCompletion(true);

        System.exit(isDone ? 0 : 1);
    }
}
