package com.dsp.commonResources;


import com.dsp.utils.GeneralUtils;
import com.dsp.utils.Stemmer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Biarc implements WritableComparable<Biarc> {
    private Text rootLexeme;
    private Text biarcWords;
    private Text dependencyIndices;
    private Text features;
    private LongWritable totalCount;

    //constructors
    public Biarc() {
        GeneralUtils.logPrint("Biarc: called empty constructor");
        this.rootLexeme = new Text("");
        this.biarcWords = new Text("");
        this.dependencyIndices = new Text("");
        this.features = new Text("");
        this.totalCount = new LongWritable(-1);
    }

    public Biarc(String rootLexeme, String biarcWords, List<String> dependencyIndices, List<String> features, long totalCount) {
        this.rootLexeme = new Text(rootLexeme);
        this.biarcWords = new Text(biarcWords);
        this.dependencyIndices = new Text(dependencyIndices.toString());
        this.features = new Text(features.toString());
        this.totalCount = new LongWritable(totalCount);
    }

    public Text getRootLexeme() {
        return rootLexeme;
    }

    public void setRootLexeme(Text rootLexeme) {
        this.rootLexeme = rootLexeme;
    }

    public Text getBiarcWords() {
        return biarcWords;
    }

    public void setBiarcWords(Text biarcWords) {
        this.biarcWords = biarcWords;
    }

    public List<String> getDependencies() {
        if(dependencyIndices.toString().equals("[]")){
            return new ArrayList<>();
        }
        return Arrays.asList(dependencyIndices.toString().substring(1, dependencyIndices.toString().length()-1).split(", "));
    }

    public void setDependencies(Text dependencies) {
        this.dependencyIndices = dependencies;
    }

    public List<String> getFeatures() {
        if(features.toString().equals("[]")){
            return new ArrayList<>();
        }
        return Arrays.asList(features.toString().substring(1, features.toString().length()-1).split(", "));
    }

    public void setFeatures(Text features) {
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
        dataOutput.writeUTF(this.biarcWords.toString());
        dataOutput.writeUTF(this.dependencyIndices.toString());
        dataOutput.writeUTF(this.features.toString());
        dataOutput.writeLong(this.totalCount.get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.rootLexeme = new Text(dataInput.readUTF());
        this.biarcWords = new Text(dataInput.readUTF());
        this.dependencyIndices = new Text(dataInput.readUTF());
        this.features = new Text(dataInput.readUTF());
        this.totalCount = new LongWritable(dataInput.readLong());
    }

    @Override
    public String toString(){
       return String.format("%s\t%s\t%s\t%s\t%s", rootLexeme.toString(), biarcWords.toString(), dependencyIndices.toString(), features.toString(), totalCount.get());
    }

    // biarc format: head_word<TAB>syntactic-ngram<TAB>total_count<TAB>counts_by_year
    // syntactic-ngram format: space separated values -> word/pos-tag/dep-label/head-index
    public static Biarc parseBiarc(String line, Stemmer stemmer){
        String[] words = line.split("\t");
        String root =  GeneralUtils.stem(words[0], stemmer);
        String[] biarc = words[1].split(" ");
        List<String[]> biarcWords = new ArrayList<>();
        for (int i = 0; i < biarc.length; i++) {
            String[] s = biarc[i].split("/");
            biarcWords.add(s);
        }
        // find all features that depend on the root word
        List<String> features = new ArrayList<>();
        List<String> stemmedWords = new ArrayList<>();
        List<String> dependencyIndices = new ArrayList<>();
        for (String[] biarcWord : biarcWords) {
            String stemmedWord = GeneralUtils.stem(biarcWord[0], stemmer);
            stemmedWords.add(stemmedWord);
            String feature = stemmedWord + "-" + biarcWord[2];
            features.add(feature);
            dependencyIndices.add(biarcWord[3]);
        }

        StringBuilder sb = new StringBuilder();
        for (int i=0; i<stemmedWords.size(); i++) {
            sb.append(stemmedWords.get(i));
            if(i < stemmedWords.size() - 1) {
                sb.append("\t");
            }
        }

        long totalCount = Long.parseLong(words[2]);
        return new Biarc(root,sb.toString(),dependencyIndices,features,totalCount);
    }

}









