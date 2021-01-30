package com.dsp.step5Join1;

import com.dsp.utils.GeneralUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.UUID;

public class Step5Join1 {

    public static class MapperClass extends Mapper<Text, LongWritable, Text, Text> {

        @Override
        public void map(Text key, LongWritable count, Context context) throws IOException,  InterruptedException {
            //we emit key=lexeme/<feature,lexeme> (depending on input file of K-V pair) and value = tag (L/LF) + count of key
            //the partitioner sends files according to only the lexeme word
            String value = "";
            if(key.toString().split(",").length == 1){ //String.split returns array with the original string if split is not possible
                value = "L\t" + count.toString(); // key is from count(L=l)
            }
            else{
                value = "LF\t" + count.toString(); // key is from count(F=f,L=l)
            }

            GeneralUtils.logPrint("in step5 map: emitting key = "+ key.toString() + ", value = " + value);
            context.write(key,new Text(value));
        }
    }

    public static class ReducerClass extends Reducer<Text, Text,Text, Text> {

        public static String classUuid = UUID.randomUUID().toString().replace("-","");

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String countLl = "null";

            String uuid = UUID.randomUUID().toString().replace("-","");

            for(Text value : values){
                String[] splittedValue = value.toString().split("\t");
                GeneralUtils.logPrint("reducer id: " + classUuid + " in step5 reduce uuid: " + uuid +": received key = " + key.toString() + " value = " + value.toString());
                //we made sure count(L=l) came first (before all count(F=f,L=l)
                if(splittedValue[0].equals("L")){
                    countLl = splittedValue[1];
                }
                //if the value is of tag "LF", i.e it is of count(F=f,L=l)
                else{
                    //emit key = Feature, value=<count(F=f,L=l),count(L=l)>
                    String feature = key.toString().split(",")[1];
                    String countLF = splittedValue[1];
                    context.write(new Text(feature), new Text(countLF+"\t"+countLl));
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

    // A comparator for text, to make sure the K-V pair of the lexeme count (Count(L=l)) comes to the reducer before
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
            //split key with length of 1 is of count(L=l), so it comes first in order
            if(split1.length == 1 && split2.length == 2){
                GeneralUtils.logPrint("in TextComparator: received key = " + split1[0]);
                return -1;
            }
            else if(split1.length == 2 && split2.length == 1){
                return 1;
            }
            else return 0;
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

        Job job = new Job(conf, "step5Join1");
        job.setJarByClass(Step5Join1.class);
        job.setMapperClass(Step5Join1.MapperClass.class);
        job.setPartitionerClass(Step5Join1.PartitionerClass.class);
        job.setGroupingComparatorClass(Step5Join1.GroupingComparator.class);
        job.setSortComparatorClass(Step5Join1.TextKeyComparator.class);
        job.setReducerClass(Step5Join1.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(s3BucketUrl+joinInputs[0]), SequenceFileInputFormat.class, Step5Join1.MapperClass.class);
        MultipleInputs.addInputPath(job, new Path(s3BucketUrl+joinInputs[1]), SequenceFileInputFormat.class, Step5Join1.MapperClass.class);
        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output));

        boolean isDone = job.waitForCompletion(true);

        System.exit(isDone ? 0 : 1);
    }
}
