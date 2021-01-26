package com.dsp.commonResources;

import com.dsp.utils.GeneralUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongPair implements WritableComparable<LongPair> {
    private LongWritable value_0;
    private LongWritable value_1;
    private Text type;

    //constructors
    public LongPair() {
        GeneralUtils.logPrint("IntegerPair: called empty constructor");
        this.value_0 = new LongWritable(Long.MIN_VALUE);
        this.value_1 = new LongWritable(Long.MIN_VALUE);
        this.type = new Text("");
    }

    public LongPair(LongWritable value_0, LongWritable value_1) {
        this.value_0 = value_0;
        this.value_1 = value_1;
        this.type = new Text("");
    }

    public LongWritable getValue_0() {
        return value_0;
    }

    public LongWritable getValue_1() {
        return value_1;
    }

    public void setType(Text type) {
        this.type = type;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.value_0.get());
        dataOutput.writeLong(this.value_1.get());
        dataOutput.writeUTF(this.type.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value_0 = new LongWritable(dataInput.readLong());
        this.value_1 = new LongWritable(dataInput.readLong());
        this.type = new Text(dataInput.readUTF());
    }

    @Override
    public int compareTo(LongPair o) {
        int res = value_0.compareTo(o.value_0);
        return res > 0 ? 1 : res < 0 ? -1 : value_1.compareTo(o.value_1);
    }

    @Override
    public String toString(){
       return String.format("%s,%s,%s", type.toString(), value_0.toString(), value_1.toString());
    }

}
