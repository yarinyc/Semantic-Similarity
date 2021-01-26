package com.dsp.application;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class LocalAppConfiguration {

    private String s3BucketName;
    private String s3BucketUrl;
    private String awsKeyPair;

    public LocalAppConfiguration() {
        readConfigFile();
        s3BucketUrl = String.format("s3://%s/", s3BucketName);
    }

    private void readConfigFile(){
        List<String> conf;
        try {
            conf = Files.readAllLines(Paths.get("resources", "config.txt"), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return;
        }
        if(conf.size() < 2 ){
            System.out.println("not enough arguments in config");
            System.exit(1);
        }
        s3BucketName = conf.get(0);
        awsKeyPair = conf.get(1);
    }

    public String getS3BucketName() {
        return s3BucketName;
    }

    public String getAwsKeyPair() {
        return awsKeyPair;
    }

    public String getS3BucketUrl() {
        return s3BucketUrl;
    }

    public void setS3BucketUrl(String s3BucketUrl) {
        this.s3BucketUrl = s3BucketUrl;
    }
}

