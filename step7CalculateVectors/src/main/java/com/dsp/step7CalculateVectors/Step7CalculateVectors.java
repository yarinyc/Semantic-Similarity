package com.dsp.step7CalculateVectors;

import com.dsp.commonResources.Pair;
import com.dsp.utils.GeneralUtils;
import com.dsp.utils.Stemmer;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Step7CalculateVectors {

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        public static Set<String> goldenStandard; // all stemmed words in GS
        public static Stemmer stemmer;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            boolean debug = Boolean.parseBoolean(context.getConfiguration().get("DEBUG"));
            GeneralUtils.setDebug(debug);

            String bucketName = context.getConfiguration().get("bucketName");
            FileSystem fileSystem = FileSystem.get(URI.create("s3://" + bucketName), context.getConfiguration());
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path("s3://" + bucketName + "/input/word-relatedness.txt"));
            Map<Pair<String, String>, Boolean> gsMap = GeneralUtils.parseGoldenStandard(fsDataInputStream);
            fsDataInputStream.close();
            fileSystem.close();

            // create a set of all words in the golden standard, after stemming
            stemmer = new Stemmer();
            goldenStandard = new HashSet<>();
            if(gsMap != null) {
                for (Pair<String, String> key : gsMap.keySet()) {
                    goldenStandard.add(GeneralUtils.stem(key.getFirst(), stemmer));
                    goldenStandard.add(GeneralUtils.stem(key.getSecond(), stemmer));
                }
            }
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException,  InterruptedException {
            // key=<l,f> value=[assoc_freq, assoc_prob, assoc_PMI, assoc_t-test]
            String[] splitKey = key.toString().split(",");
            String lexeme = splitKey[0];
            String feature = splitKey[1];
            if(goldenStandard.contains(lexeme)){
                String newValue = feature + "\t" + value.toString();
                context.write(new Text(lexeme), new Text(newValue));
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
            // key=l value=list of <f,[assoc_freq, assoc_prob, assoc_PMI, assoc_t-test]>
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value.toString()).append("\t\t");
            }
            // emit key=l value=list of <f,[assoc_freq, assoc_prob, assoc_PMI, assoc_t-test]> (each feature separated by '\t\t')
            context.write(key, new Text(sb.toString()));
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

        System.exit(isDone ? 0 : 1);
    }
}
