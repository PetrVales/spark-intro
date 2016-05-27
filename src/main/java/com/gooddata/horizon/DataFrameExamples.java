package com.gooddata.horizon;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;

import java.net.URISyntaxException;

import static org.apache.spark.sql.functions.*;

public class DataFrameExamples extends WithSparkContext {

    public static void main(String[] args) throws URISyntaxException {
        setupContext();

        wholeExample();

        closeContext();
    }

    static void wholeExample() {
        final DataFrame csv = loadFile();
        csv.show();

        final DataFrame codeAgg = codeAgg(csv);
        codeAgg.show();

        final DataFrame sqlAgg = sqlAgg(csv);
        sqlAgg.show();

        codeAgg.explain();
        sqlAgg.explain();

        codeAgg.repartition(1).write()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("output");
    }

    static DataFrame loadFile() {
        return sqlContext.read()
                            .format("com.databricks.spark.csv")
                            .option("header", "true")
                            .load(DataFrameExamples.class.getResource("/data.csv").toString());
    }

    static DataFrame codeAgg(DataFrame csv) {
        final GroupedData grouped = csv.groupBy(col("a"));
        return grouped.agg(sum("b"), avg("d"));
    }

    static DataFrame sqlAgg(DataFrame csv) {
        csv.registerTempTable("csvFile");
        return sqlContext.sql("SELECT a, sum(b), avg(d) FROM csvFile GROUP BY a");
    }


}
