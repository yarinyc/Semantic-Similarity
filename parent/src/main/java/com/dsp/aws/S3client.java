//package com.dsp.aws;
//
//import com.amazonaws.regions.Regions;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.s3.model.*;
//import com.dsp.utils.GeneralUtils;
//import org.apache.commons.io.FileUtils;
//
//import java.io.*;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.stream.Collectors;
//
//public class S3client {
//
//    private static final Regions REGION = Regions.US_EAST_1;
//    private final AmazonS3 s3;
//
//
//    public S3client() {
//        s3 = AmazonS3ClientBuilder
//                .standard()
//                .withRegion(REGION)
//                .build();
//    }
//
//    // create an s3 bucket.
//    public boolean createBucket(String bucketName) {
//        try {
//            s3.createBucket(bucketName);
//        } catch (Exception e) {
//            GeneralUtils.printStackTrace(e);
//            return false;
//        }
//        return true;
//    }
//
//    public boolean deleteBucket(String bucketName){
//        try {
//            s3.deleteBucket(bucketName);
//        } catch (Exception e) {
//            GeneralUtils.printStackTrace(e);
//            return false;
//        }
//        return true;
//    }
//
//    // add a key/value pair to an S3 bucket from given file located in inFilePath.
//    public boolean putObject(String bucketName, String bucketKey, String inFilePath) {
//        try {
//            s3.putObject(
//                    bucketName,
//                    bucketKey,
//                    new File(inFilePath)
//            );
//        } catch (Exception e) {
//            GeneralUtils.printStackTrace(e);
//            return false;
//        }
//        return true;
//    }
//
//    // add a key/value pair to an S3 bucket from given file located in inFilePath.
//    public boolean putObjectFromMemory(String bucketName, String bucketKey, String value) {
//        try {
//            s3.putObject(
//                    bucketName,
//                    bucketKey,
//                    value
//            );
//        } catch (Exception e) {
//            GeneralUtils.printStackTrace(e);
//            return false;
//        }
//        return true;
//    }
//
//    public boolean deleteObject(String bucketName, String bucketKey){
//        try{
//            s3.deleteObject(bucketName, bucketKey);
//        } catch(Exception e){
//            GeneralUtils.printStackTrace(e);
//            return false;
//        }
//        return true;
//    }
//
//    // read the value of bucketKey in the S3 bucket and save it to the file in outFilePath
//    public boolean getObject(String bucketName, String bucketKey, String outFilePath) {
//        try {
//            S3Object s3object = s3.getObject(bucketName, bucketKey);
//            S3ObjectInputStream inputStream = s3object.getObjectContent();
//            FileUtils.copyInputStreamToFile(inputStream, new File(outFilePath));
//        } catch (Exception e) {
//            GeneralUtils.printStackTrace(e);
//            return false;
//        }
//        return true;
//    }
//
//    // read the value of bucketKey in the S3 bucket and save it to the file in outFilePath
//    public String getObjectToMemory(String bucketName, String bucketKey) {
//        S3Object s3Object = s3.getObject(bucketName, bucketKey);
//        S3ObjectInputStream stream = s3Object.getObjectContent();
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream));
//        StringBuilder text = new StringBuilder();
//        String temp;
//
//        try {
//            while((temp = bufferedReader.readLine()) != null){
//                text.append(temp);
//                text.append("\n");
//            }
//            bufferedReader.close();
//            stream.close();
//        } catch (IOException e) {
//            GeneralUtils.printStackTrace(e);
//            return null;
//        }
//        return text.substring(0, text.toString().length()-1);
//    }
//
//    public List<String> getAllObjectsKeys(String bucketName, String prefix){
//        List<String> keys = new ArrayList<>();
//        ObjectListing objectListing = s3.listObjects(bucketName);
//        for(S3ObjectSummary os : objectListing.getObjectSummaries()) {
//            if(prefix.equals(os.getKey().substring(0,prefix.length()))){
//                keys.add(os.getKey());
//            }
//        }
//        return keys;
//    }
//
//    public List<String> getAllBucketNames(){
//        List<Bucket> buckets = s3.listBuckets();
//        return buckets
//                .stream()
//                .map(Bucket::getName)
//                .collect(Collectors.toList());
//    }
//}
//
