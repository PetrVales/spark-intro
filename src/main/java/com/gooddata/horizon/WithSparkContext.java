package com.gooddata.horizon;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

class WithSparkContext {

    protected static JavaSparkContext context;
    protected static SQLContext sqlContext;

    protected static void setupContext() {
        context = createContext();
        sqlContext = new SQLContext(context);
    }

    private static JavaSparkContext createContext() {
        SparkConf conf = new SparkConf()
                .setAppName("test")
                .setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

    protected static void closeContext() {
        context.close();
    }

    protected static DataFrame readTblAsDataFrame(String file) {
        return sqlContext
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("delimiter", "|")
                .load(DataFrameExamples.class.getResource(file).toString());
    }
}
