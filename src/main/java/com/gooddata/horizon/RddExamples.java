package com.gooddata.horizon;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RddExamples extends WithSparkContext {

    public static void main(String[] args) {
        setupContext();

        final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 6, 6);
        final JavaRDD<Integer> rdd = context.parallelize(list);

        System.out.println(rdd.distinct().reduce((a, b) -> a + b));

        final List<String> text = Arrays.asList(
            "Episode IV: A NEW HOPE",
            "It is a period of civil war. Rebel spaceships, striking from a hidden base, have won their first victory against the evil Galactic Empire.",
            "During the battle, Rebel spies managed to steal secret plans to the Empire's ultimate weapon, the DEATH STAR, an armored space station with enough power to destroy an entire planet.",
            "Pursued by the Empire's sinister agents, Princess Leia races home aboard her starship, custodian of the stolen plans that can save her people and restore freedom to the galaxy...."
        );
        final JavaRDD<String> textRdd = context.parallelize(text);

        System.out.print(wordCounts(textRdd).take(10));

        closeContext();
    }

    public static long count(JavaRDD rdd) {
        return rdd.count();
    }

    public static JavaRDD<Integer> distinct(JavaRDD<Integer> rdd) {
        return rdd.distinct();
    }

    public static JavaRDD<Integer> multiply(JavaRDD<Integer> rdd, final int x) {
        return rdd.map(a -> a * x);
    }

    public static int sum(JavaRDD<Integer> rdd) {
        return rdd.reduce((a, b) -> a + b);
    }

    static JavaPairRDD<Integer, String> wordCounts(JavaRDD<String> rdd) {
        return rdd.flatMap(line -> Arrays.asList(line.split(" ")))
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((a, b) -> a + b)
            .mapToPair(Tuple2::swap)
            .sortByKey(false);
    }
}
