package com.dsp.commonResources;


import com.dsp.utils.GeneralUtils;
import com.dsp.utils.Stemmer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Biarc implements WritableComparable<Biarc> {
    private Text rootLexeme;
    private TupleWritable features;
    private LongWritable totalCount;

    //constructors
    public Biarc() {
        GeneralUtils.logPrint("Biarc: called empty constructor");
        this.rootLexeme = new Text("");
        this.features = new TupleWritable();
        this.totalCount = new LongWritable(-1);
    }

    public Biarc(String rootLexeme, List<String> features, long totalCount) {
        this.rootLexeme = new Text(rootLexeme);
        this.totalCount = new LongWritable(totalCount);
//        List<Text> textList = features.stream().map(s -> new Text(s)).collect(Collectors.toList());
//        Text[] textArray = new Text[textList.size()];
//        textArray = textList.toArray(textArray);
//        this.features = new TupleWritable(textArray);
        Object[] textArray = features.stream().map(s -> new Text(s)).toArray(); // check if this works
        this.features = new TupleWritable((Text[]) textArray);
    }

    public Text getRootLexeme() {
        return rootLexeme;
    }

    public void setRootLexeme(Text rootLexeme) {
        this.rootLexeme = rootLexeme;
    }

    public TupleWritable getFeatures() {
        return features;
    }

    public void setFeatures(TupleWritable features) {
        this.features = features;
    }

    public LongWritable getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(LongWritable totalCount) {
        this.totalCount = totalCount;
    }

    @Override
    public int compareTo(Biarc o) {
        return  rootLexeme.compareTo(o.rootLexeme);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.rootLexeme.toString());
        features.write(dataOutput);
        dataOutput.writeLong(this.totalCount.get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.rootLexeme = new Text(dataInput.readUTF());
        features.readFields(dataInput);
        this.totalCount = new LongWritable(dataInput.readLong());
    }

    @Override
    public String toString(){
       return String.format("%s,%s,%s", rootLexeme.toString(), totalCount.get(), features.toString());
    }

    // biarc format: head_word<TAB>syntactic-ngram<TAB>total_count<TAB>counts_by_year
    // syntactic-ngram format: space separated values -> word/pos-tag/dep-label/head-index
    public static Biarc parseBiarc(String line, Stemmer stemmer){
        String[] words = line.split("\t");
        String root =  GeneralUtils.stem(words[0], stemmer);
        String[] biarc = words[1].split(" ");
        List<String[]> biarcWords = new ArrayList<>();
        int rootIndex = -1; // index of the root in the biarc
        for (int i = 0; i < biarc.length; i++) {
            String[] s = biarc[i].split("/");
            if(s[0].equals(root)){
                rootIndex = i+1; // in the dataset, position in the biarc starts from 1
            }
            biarcWords.add(s);
        }
        // find all features that depend on the root word
        List<String> features = new ArrayList<>();
        for (String[] biarcWord : biarcWords) {
            int dependencyIndex = Integer.parseInt(biarcWord[3]);
            if(dependencyIndex == rootIndex){
                String stemmedWord = GeneralUtils.stem(biarcWord[0], stemmer);
                String feature = stemmedWord + "-" + biarcWord[2];
                features.add(feature);
            }
        }

        long totalCount = Long.parseLong(words[2]);
        return new Biarc(root,features,totalCount);
    }

}









