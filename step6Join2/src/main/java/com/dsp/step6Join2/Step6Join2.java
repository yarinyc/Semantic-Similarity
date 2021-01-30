package com.dsp.step6Join2;

import com.dsp.commonResources.AssocCalculator;
import com.dsp.utils.GeneralUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Step6Join2 {

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            boolean debug = Boolean.parseBoolean(context.getConfiguration().get("DEBUG"));
            GeneralUtils.setDebug(debug);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException,  InterruptedException {
            //we emit key=feature/<feature,lexeme> (depending on input file of K-V pair) and value = tag (F/LF) + count of key
            //the partitioner sends files according to only the lexeme word
            String val;
            if(key.toString().split(",").length == 1){ //String.split returns array with the original string if split is not possible
                val = "F\t" + value.toString(); // key is from count(F=f)
            }
            else{
                val = "LF\t" + key.toString() + "\t"+ value.toString(); // key is from count(F=f,L=l): value will include the <l,f> as well
            }

            GeneralUtils.logPrint("in step6 map: emitting key = "+ key.toString() + ", value = " + val);
            context.write(key,new Text(val));
        }
    }

    public static class ReducerClass extends Reducer<Text, Text,Text, Text> {

        private static Long countAllLexemes; //count(L)
        private static Long countAllFeatures; //count(F)

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            boolean debug = Boolean.parseBoolean(context.getConfiguration().get("DEBUG"));
            GeneralUtils.setDebug(debug);

            //get count(L) & count(F) from s3
            String bucketName = context.getConfiguration().get("bucketName");
            FileSystem fileSystem = FileSystem.get(URI.create("s3://" + bucketName), context.getConfiguration());

            FSDataInputStream fsDataInputStream = fileSystem.open(new Path(("s3://" + bucketName + "/COUNTL")));
            String input = IOUtils.toString(fsDataInputStream, StandardCharsets.UTF_8);
            countAllLexemes = Long.valueOf(input);
            GeneralUtils.logPrint("in setup step6: count(L)=" + countAllLexemes);

            fsDataInputStream = fileSystem.open(new Path(("s3://" + bucketName + "/COUNTF")));
            input = IOUtils.toString(fsDataInputStream, StandardCharsets.UTF_8);
            countAllFeatures = Long.valueOf(input);
            GeneralUtils.logPrint("in setup step6: count(F)=" + countAllFeatures);

            fsDataInputStream.close();
            fileSystem.close();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String countF = "dummy-value";
            for(Text value : values){
                String[] splitValue = value.toString().split("\t");
                GeneralUtils.logPrint("in step6 reduce: received key = " + key.toString() + " value = " + value.toString());
                //we made sure count(F=f) came first (before all count(F=f,L=l)
                if(splitValue[0].equals("F")){
                    countF = splitValue[1];
                }
                //if the value is of tag "LF", i.e it is of <count(F=f,L=l),count(L=l)>
                else{
                    String newKey = splitValue[1]; // <lexeme,feature>
                    String[] counters = splitValue[2].split(",");
                    //newValue=<count(F=f,L=l),count(L=l),count(F=f)>
                    String newValue = counters[0]+","+countF;
                    context.write(new Text(newKey), new Text(newValue));
                    //calculate all assoc values according to the formulas in the article
//                    List<Long> counts = Arrays.stream(newValue.split(",")).map(Long::parseLong).collect(Collectors.toList());
//                    AssocCalculator assocCalculator = new AssocCalculator(countAllLexemes, countAllFeatures, counts.get(1), counts.get(2), counts.get(0));
//                    List<Number> assocs = assocCalculator.getAllAssocValues();
//                    context.write(new Text(newKey), new Text(assocs.toString()));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            // foreign key is the lexeme (split key at index 0)
            String lexeme = key.toString().split(",")[0];
            return Math.abs(lexeme.hashCode() % numPartitions);
        }
    }

    // A comparator for text, to make sure the K-V pair of the lexeme count (Count(F=f)) comes to the reducer before
    // all <lexeme,feature> counts (Count(L=l,F=f))
    public static class TextKeyComparator extends WritableComparator {
        protected TextKeyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            Text key1 = (Text) o1;
            Text key2 = (Text) o2;
            String[] split1 = key1.toString().split(",");
            String[] split2 = key2.toString().split(",");
            int compareVal = split1[0].compareTo(split2[0]);
            //split key with length of 1 is of count(F=f), so it comes first in order
            if(compareVal==0){
                if(split1.length == 1 && split2.length == 2){
                    GeneralUtils.logPrint("in TextComparator: received key = " + split1[0]);
                    return -1;
                }
                else if(split1.length == 2 && split2.length == 1){
                    return 1;
                }
                else return 0;
            }
            else{
                return compareVal;
            }
        }
    }

    // Grouping comparator to make sure all lexeme and <lexeme,feature> keys arrive to the same reducer according only to lexeme
    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
            Text key1 = (Text) o1;
            Text key2 = (Text) o2;
            String lexeme1 = key1.toString().split(",")[0];
            String lexeme2 = key2.toString().split(",")[0];
            GeneralUtils.logPrint("in GroupingComparator: received keys = " + key1.toString() + " " + key2.toString() + " returned " + lexeme1.compareTo(lexeme2));
            return lexeme1.compareTo(lexeme2);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String s3BucketName = args[1];
        String s3BucketUrl = String.format("s3://%s/", s3BucketName);
        String input = args[2];
        String[] joinInputs = input.split("\t");
        String output = args[3];

        // set debug flag for logging
        boolean debug = Boolean.parseBoolean(args[4]);
        GeneralUtils.setDebug(debug);

        Configuration conf = new Configuration();
        conf.set("DEBUG", Boolean.toString(debug));
        conf.set("bucketName", s3BucketName);

        Job job = new Job(conf, "step6Join2");
        job.setJarByClass(Step6Join2.class);
        job.setMapperClass(Step6Join2.MapperClass.class);
        job.setPartitionerClass(Step6Join2.PartitionerClass.class);
        job.setGroupingComparatorClass(Step6Join2.GroupingComparator.class);
        job.setSortComparatorClass(Step6Join2.TextKeyComparator.class);
        job.setReducerClass(Step6Join2.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(s3BucketUrl+joinInputs[0]), SequenceFileInputFormat.class, Step6Join2.MapperClass.class);
        MultipleInputs.addInputPath(job, new Path(s3BucketUrl+joinInputs[1]), SequenceFileInputFormat.class, Step6Join2.MapperClass.class);
        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output));

        boolean isDone = job.waitForCompletion(true);

        System.exit(isDone ? 0 : 1);
    }
}
