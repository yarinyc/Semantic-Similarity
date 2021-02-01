package com.dsp.commonResources;

import java.util.Objects;

/** A generic class for pairs.
 *
 */
public class Pair<A, B> {

    private final A first;
    private final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }

    public String toString() {
        return "<" + first + "," + second + ">";
    }

    public boolean equals(Object other) {
            return other instanceof Pair<?,?> &&
                        Objects.equals(first, ((Pair<?,?>)other).first) &&
                        Objects.equals(second, ((Pair<?,?>)other).second);
    }

    public int hashCode() {
        if(first == null){
            return (second == null) ? 0 : second.hashCode() + 1;
        }
        else if(second == null){
            return first.hashCode() + 2;
        }
        else{
            return first.hashCode() * 17 + second.hashCode();
        }
    }

    public static <A,B> Pair<A,B> of(A a, B b) {
        return new Pair<>(a,b);
    }
}
