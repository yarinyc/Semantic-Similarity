package com.dsp.application;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.io.IOException;

public class LocalApplication {

    private final static String s3InputPath = "s3://s3bucket-d797b2f9e96963c1/input/test_input_dsp3.txt";
    private final static String EMR_EC2_DEFAULT_ROLE = "EMR_EC2_DefaultRole";
    private final static String EMR_DEFAULT_ROLE = "EMR_DefaultRole";
    private final static int NUM_OF_INSTANCES = 2;
    private final static boolean DELETE_OUTPUTS = true;
    private final static boolean DEBUG = true;

    public static void main(String[] args){

        LocalAppConfiguration localAppConfiguration = new LocalAppConfiguration();

        if(DELETE_OUTPUTS) {
            cleanS3Bucket(localAppConfiguration);
        }

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar(localAppConfiguration.getS3BucketUrl() + "jars/step1ReformatBiarcs.jar")
                .withArgs(localAppConfiguration.getS3BucketName(), s3InputPath, "step_1_results/", Boolean.toString(DEBUG))
                .withMainClass("Step1ReformatBiarcs");

        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar(localAppConfiguration.getS3BucketUrl() + "jars/step2CountLexemesFeatures.jar")
                .withArgs(localAppConfiguration.getS3BucketName(), "step_1_results/", "step_2_results/", Boolean.toString(DEBUG))
                .withMainClass("Step2CountLexemesFeatures");

        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar(localAppConfiguration.getS3BucketUrl() + "jars/step3CountFeatures.jar")
                .withArgs(localAppConfiguration.getS3BucketName(),"step_2_results/", "step_3_results/", Boolean.toString(DEBUG))
                .withMainClass("Step3CountFeatures");

        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar(localAppConfiguration.getS3BucketUrl() + "jars/step4CountLexemes.jar")
                .withArgs(localAppConfiguration.getS3BucketName(), "step_1_results/", "step_4_results/", Boolean.toString(DEBUG))
                .withMainClass("Step4CountLexemes");

//        HadoopJarStepConfig hadoopJarStep5 = new HadoopJarStepConfig()
//                .withJar(localAppConfiguration.getS3BucketUrl() + "jars/joinAndComputeProbability.jar")
//                .withArgs(localAppConfiguration.getS3BucketName())
//                .withMainClass("JoinAndComputeProbability");
//
//        HadoopJarStepConfig hadoopJarStep6 = new HadoopJarStepConfig()
//                .withJar(localAppConfiguration.getS3BucketUrl() + "jars/sortOutput.jar")
//                .withArgs(localAppConfiguration.getS3BucketName())
//                .withMainClass("SortOutput");


        StepConfig stepConfig1 = new StepConfig()
                .withName("parse biarcs")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig2 = new StepConfig()
                .withName("count <lexeme, feature>")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig3 = new StepConfig()
                .withName("count features")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig stepConfig4 = new StepConfig()
                .withName("count lexemes")
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

//        StepConfig stepConfig5 = new StepConfig()
//                .withName("join P and Trigrams")
//                .withHadoopJarStep(hadoopJarStep5)
//                .withActionOnFailure("TERMINATE_JOB_FLOW");
//
//        StepConfig stepConfig6 = new StepConfig()
//                .withName("sort output")
//                .withHadoopJarStep(hadoopJarStep6)
//                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(NUM_OF_INSTANCES)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("3.2.1")
                .withEc2KeyName(localAppConfiguration.getAwsKeyPair())
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("dsp3-Biarcs")
                .withInstances(instances)
                .withSteps(stepConfig1 , stepConfig2, stepConfig3, stepConfig4/*, stepConfig5, stepConfig6*/)
                .withServiceRole(EMR_DEFAULT_ROLE)
                .withJobFlowRole(EMR_EC2_DEFAULT_ROLE)
                .withReleaseLabel("emr-6.2.0")
                .withLogUri(localAppConfiguration.getS3BucketUrl() + "logs/");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    // clean the s3 bucket from logs and mapReduce step outputs
    private static void cleanS3Bucket(LocalAppConfiguration localAppConfiguration) {
        String bucketName = localAppConfiguration.getS3BucketName();
        try {
            Runtime.getRuntime().exec("aws s3 rm s3://"+ bucketName +"/COUNTL");
            Runtime.getRuntime().exec("aws s3 rm s3://"+ bucketName +"/COUNTF");
            Runtime.getRuntime().exec("aws s3 rm --recursive s3://"+ bucketName +"/logs/");
            Runtime.getRuntime().exec("aws s3 rm --recursive s3://"+ bucketName +"/step_1_results/");
            Runtime.getRuntime().exec("aws s3 rm --recursive s3://"+ bucketName +"/step_2_results/");
            Runtime.getRuntime().exec("aws s3 rm --recursive s3://"+ bucketName +"/step_3_results/");
            Runtime.getRuntime().exec("aws s3 rm --recursive s3://"+ bucketName +"/step_4_results/");
//            Runtime.getRuntime().exec("aws s3 rm --recursive s3://"+ bucketName +"/step_5_results/");
//            Runtime.getRuntime().exec("aws s3 rm --recursive s3://"+ bucketName +"/step_6_results/");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}