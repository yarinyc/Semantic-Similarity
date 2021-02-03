package com.dsp.commonResources;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class SimilarityCalculator {

    private Map<String,Number> vector1;
    private Map<String,Number> vector2;

    public SimilarityCalculator(Map<String,Number> vector1, Map<String,Number> vector2) {
        this.vector1 = vector1;
        this.vector2 = vector2;
    }

    // similarity equation 9
    public double simManhattan (){

        return 0;
    }

    // similarity equation 10
    public double simEuclidean (){

        return 0;
    }

    // similarity equation 11
    public double simCosine (){

        return 0;
    }

    // similarity equation 13
    public double simJacard (){

        return 0;
    }

    // similarity equation 15
    public double simDice (){

        return 0;
    }

    // similarity equation 17
    public double simJS (){

        return 0;
    }

    //return all assoc values: [assoc_freq, assoc_prob, assoc_PMI, assoc_t-test]
    public List<Double> getAllSimilarities(){
        List<Double> similarities = new ArrayList<>();
        similarities.add(simManhattan());
        similarities.add(simEuclidean());
        similarities.add(simCosine());
        similarities.add(simJacard());
        similarities.add(simDice());
        similarities.add(simJS());
        return similarities;
    }
}
