package com.gooddata.horizon;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ContextExamples {

    private static final String APP_NAME = "App name";
    private static final String LOCAL_MASTER = "local";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(APP_NAME)
                .setMaster(LOCAL_MASTER);
        new JavaSparkContext(conf);
    }
    
}
