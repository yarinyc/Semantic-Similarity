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
        public void setup(Context context) throws IOException, InterruptedException {
            boolean debug = Boolean.parseBoolean(context.getConfiguration().get("DEBUG"));
            GeneralUtils.setDebug(debug);
        }

        @Override
        public void map(Text key, LongWritable count, Context context) throws IOException,  InterruptedException {
            //we emit key=lexeme/<feature,lexeme> (depending on input file of K-V pair) and value = tag (L/LF) + count of key
            //the partitioner sends files according to only the lexeme word
            String value;
            if(key.toString().split(",").length == 1){ //String.split returns array with the original string if split is not possible
                value = "L\t" + count.toString(); // key is from count(L=l)
            }
            else{
                value = "LF\t" + key.toString() + "\t"+ count.toString(); // key is from count(F=f,L=l): value will include the <l,f> as well
            }

            GeneralUtils.logPrint("in step5 map: emitting key = "+ key.toString() + ", value = " + value);
            context.write(key,new Text(value));
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
            String countL = "dummy-value";
            for(Text value : values){
                String[] splitValue = value.toString().split("\t");
                GeneralUtils.logPrint("in step5 reduce: received key = " + key.toString() + " value = " + value.toString());
                //we made sure count(L=l) came first (before all count(F=f,L=l)
                if(splitValue[0].equals("L")){
                    countL = splitValue[1];
                }
                //if the value is of tag "LF", i.e it is of count(F=f,L=l)
                else if(!countL.equals("dummy-value")){
                    //emit key = <l,f>, value=<count(F=f,L=l),count(L=l)>
                    String newKey = splitValue[1]; //<lexeme,feature>
                    String countLF = splitValue[2];
                    context.write(new Text(newKey), new Text(countLF+"\t"+countL));
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
            int compareVal = split1[0].compareTo(split2[0]);
            //split key with length of 1 is of count(L=l), so it comes first in order
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
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(s3BucketUrl+joinInputs[0]), SequenceFileInputFormat.class, Step5Join1.MapperClass.class);
        MultipleInputs.addInputPath(job, new Path(s3BucketUrl+joinInputs[1]), SequenceFileInputFormat.class, Step5Join1.MapperClass.class);
        FileOutputFormat.setOutputPath(job, new Path(s3BucketUrl+output));

        boolean isDone = job.waitForCompletion(true);

        System.exit(isDone ? 0 : 1);
    }
}
