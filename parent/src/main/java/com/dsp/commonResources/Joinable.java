package com.dsp.commonResources;

import org.apache.hadoop.io.WritableComparable;

public abstract class Joinable<T> implements WritableComparable<T> {
    public abstract String getType();
}
