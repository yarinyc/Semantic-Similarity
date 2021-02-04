package com.dsp.commonResources;

import com.dsp.utils.GeneralUtils;

import java.util.*;


public class SimilarityCalculator {

    private HashMap<String,Double> vector1;
    private HashMap<String,Double> vector2;
//    private Set<String> intersectionSet; // keys (features) both in vector1 and vector2
    private HashSet<String> unionSet; // all possible keys (features)
//    private Set<String> vector1DifferenceSet; // keys (features) in vector1 that are not in vector2
//    private Set<String> vector2DifferenceSet; // keys (features) in vector2 that are not in vector1

    public SimilarityCalculator(HashMap<String,Double> vector1, HashMap<String,Double> vector2) {
        this.vector1 = vector1;
        this.vector2 = vector2;
        // compute relevant feature sets
//        this.intersectionSet = vector1.keySet();
//        intersectionSet.retainAll(vector2.keySet());
        this.unionSet = new HashSet<>(vector1.keySet());
        unionSet.addAll(vector2.keySet());
//        this.vector1DifferenceSet = vector1.keySet();
//        vector1DifferenceSet.removeAll(vector2.keySet());
//        this.vector2DifferenceSet = vector2.keySet();
//        vector2DifferenceSet.removeAll(vector1.keySet());
    }

    // similarity equation 9 - Manhattan distance
    public double simManhattan(){
        double similarity = 0.0;

//        for(String feature : intersectionSet){
//            similarity += Math.abs((Double) vector1.get(feature) - (Double)vector2.get(feature));
//        }
//
//        for(String feature : vector1DifferenceSet){
//            similarity += Math.abs((Double) vector1.get(feature));
//        }
//
//        for(String feature : vector2DifferenceSet){
//            similarity += Math.abs((Double) vector2.get(feature));
//        }

        for(String feature : unionSet){
            similarity += Math.abs(vector1.getOrDefault(feature, 0.0) - vector2.getOrDefault(feature, 0.0));
        }

        return similarity;
    }

    // similarity equation 10 - Euclidean distance
    public double simEuclidean(){
        double similarity = 0.0;

//        for(String feature : intersectionSet){
//            similarity += Math.pow((Double) vector1.get(feature) - (Double)vector2.get(feature), 2);
//        }
//
//        for(String feature : vector1DifferenceSet){
//            similarity += Math.pow((Double) vector1.get(feature), 2);
//        }
//
//        for(String feature : vector2DifferenceSet){
//            similarity += Math.pow((Double) vector2.get(feature), 2);
//        }

        for(String feature : unionSet){
            similarity += Math.pow((vector1.getOrDefault(feature, 0.0) - vector2.getOrDefault(feature, 0.0)), 2.0);
        }

        return Math.sqrt(similarity);
    }

    // similarity equation 11 - Cosine distance
    public double simCosine(){
        double numerator = 0.0;
        double denominatorTerm1 = 0.0;
        double denominatorTerm2 = 0.0;

        for(String feature : unionSet){
            numerator += (vector1.getOrDefault(feature, 0.0) * vector2.getOrDefault(feature, 0.0));
        }

        for(String feature : vector1.keySet()){
            denominatorTerm1 += Math.pow(vector1.get(feature), 2.0);
        }

        for(String feature : vector2.keySet()){
            denominatorTerm2 += Math.pow(vector2.get(feature), 2.0);
        }

        double denominator = Math.sqrt(denominatorTerm1) * Math.sqrt(denominatorTerm2);

        GeneralUtils.logPrint("Cosine: den1 = " + denominatorTerm1 + " den2 = " + denominatorTerm2 + " final den = " + denominator);

        return numerator/denominator;
    }

    // similarity equation 13 - Jacard distance
    public double simJacard(){
        double numerator = 0.0;
        double denominator = 0.0;

//        for(String feature : intersectionSet){
//            numerator += Math.min((Double) vector1.get(feature), (Double) vector2.get(feature));
//            denominator += Math.max((Double) vector1.get(feature), (Double) vector2.get(feature));
//        }
//
//        for(String feature : vector1DifferenceSet){
//            numerator += Math.min((Double) vector1.get(feature), 0);
//            denominator += Math.max((Double) vector1.get(feature), 0);
//        }
//
//        for(String feature : vector2DifferenceSet){
//            numerator += Math.min((Double) vector2.get(feature), 0);
//            denominator += Math.max((Double) vector2.get(feature), 0);
//        }

        for(String feature : unionSet){
            numerator += Math.min(vector1.getOrDefault(feature, 0.0), vector2.getOrDefault(feature, 0.0));
            denominator += Math.max(vector1.getOrDefault(feature, 0.0), vector2.getOrDefault(feature, 0.0));
        }

        return numerator/denominator;
    }

    // similarity equation 15 - Dice distance
    public double simDice(){
        double numerator = 0.0;
        double denominator = 0.0;

        for(String feature : unionSet){
            numerator += Math.min(vector1.getOrDefault(feature, 0.0), vector2.getOrDefault(feature, 0.0));
        }
        numerator *= 2.0;

        for(String feature : unionSet){
            denominator +=  (vector1.getOrDefault(feature, 0.0) + vector2.getOrDefault(feature, 0.0));
        }

        return numerator/denominator;
    }

    // similarity equation 17 - Jensen-Shannon divergence (based on KL divergence)
    public double simJS(){
        return (KL_Divergence(1) + KL_Divergence(2));
    }

    // Kullback-Leilbler divergence
    public double KL_Divergence(int firstVectorFlag){
        double similarity = 0.0;

        if(firstVectorFlag == 1) {
            for (String feature : unionSet) {
                double Px = vector1.getOrDefault(feature, 0.0);
                double Qx = (Px + vector2.getOrDefault(feature, 0.0)) / 2.0;

                GeneralUtils.logPrint("KL, P = " + Px + " Q = " + Qx);
//                if (Qx == 0.0) {
//                    Qx = 1.0;
//                }
                // Qx should never be 0 (as the feature x came from one of the two vectors)
                // so there is no risk of division by 0
                similarity += Px * Math.log(Px / Qx);
            }
        }

        else{
            for (String feature : unionSet) {
                double Px = vector2.getOrDefault(feature, 0.0);
                double Qx = (Px + vector1.getOrDefault(feature, 0.0)) / 2.0;

                GeneralUtils.logPrint("KL, P = " + Px + " Q = " + Qx);
//                if (Qx == 0.0) {
//                    Qx = 1.0;
//                }
                // Qx should never be 0 (as the feature x came from one of the two vectors)
                // so there is no risk of division by 0
                similarity += Px * Math.log(Px / Qx);
            }
        }

        return similarity;
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
