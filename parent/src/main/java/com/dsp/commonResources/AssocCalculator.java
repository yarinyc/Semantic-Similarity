package com.dsp.commonResources;

import java.util.ArrayList;
import java.util.List;

public class AssocCalculator {

    private long countAllLexemes;
    private long countAllFeatures;
    private long countL;
    private long countF;
    private long countLF;

    public AssocCalculator(long countAllLexemes, long countAllFeatures, long countL, long countF, long countLF) {
        this.countAllLexemes = countAllLexemes;
        this.countAllFeatures = countAllFeatures;
        this.countL = countL;
        this.countF = countF;
        this.countLF = countLF;
    }

    //calculates P(F=f)=count(F=f)/count(F)
    public double probabilityF(){
        return (double)countF / countAllFeatures;
    }

    //calculates P(L=l)=count(L=l)/count(L)
    public double probabilityL(){
        return (double)countL / countAllLexemes;
    }

    //calculates P(F=f|L=l)=count(F=f, L=l)/count(L=l)
    public double probabilityFGivenL(){
        return (double) countLF / countL;
    }

    //calculates P(F=f,L=l)=count(F=f, L=l)/count(L)
    public double probabilityFAndL(){
        return (double) countLF / countAllLexemes;
    }

    //calculate assoc_freq(l, f)=count(l, f)
    public Long getAssocFreq(){
        return countLF;
    }

    //calculate assoc_prob(l, f)=P(f|l)
    public Double getAssocProb(){
        return probabilityFGivenL();
    }

    //calculate assoc_PMI(l, f)=log_2( P(l, f)/P(l)P(f) )
    public Double getAssocPMI(){
        double frac = probabilityFAndL()/(probabilityL()*probabilityF());
        return Math.log10(frac)/Math.log10(2);
    }

    // assoc_t−test(l, f)=(P(l, f)−P(l)P(f))/√(P(l)P(f))
    public Double getAssocTTest(){
        double probLMultProbF = probabilityL()*probabilityF();
        double numerator = probabilityFAndL()-probLMultProbF;
        double denominator = Math.sqrt(probLMultProbF);
        return numerator/denominator;
    }

    //return all assoc values: [assoc_freq, assoc_prob, assoc_PMI, assoc_t-test]
    public List<Number> getAllAssocValues(){
        List<Number> assocs = new ArrayList<>();
        assocs.add(getAssocFreq());
        assocs.add(getAssocProb());
        assocs.add(getAssocPMI());
        assocs.add(getAssocTTest());
        return assocs;
    }
}
