package org.ergemp.training.spark.rdd.transformations.pairRDDFunctions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class SortByKeyExample2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("rdd-tests").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines3 = sc.textFile("resources/airports.dat");
        JavaPairRDD<String, Integer> pairs3 = lines3.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> counts3 = pairs3.reduceByKey((a, b) -> a + b);
        JavaPairRDD<String, Integer> counts3sorted =counts3.sortByKey();
        List<Tuple2<String, Integer>> fetchedcountsSorted = counts3sorted.collect();
    }
}
