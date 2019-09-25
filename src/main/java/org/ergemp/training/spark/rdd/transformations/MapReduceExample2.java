package org.ergemp.training.spark.rdd.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MapReduceExample2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MapReduceExample2").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //defining transformation(map) and actions(reduce)
        JavaRDD<String> lines = sc.textFile("resources/airports.dat");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        System.out.println(totalLength);
    }
}
