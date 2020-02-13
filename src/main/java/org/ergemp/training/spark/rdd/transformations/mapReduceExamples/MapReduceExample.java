package org.ergemp.training.spark.rdd.transformations.mapReduceExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class MapReduceExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MapReduceExample").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        //JavaRDD<Integer> distData = sc.parallelize(data);
        JavaRDD<Integer> distData = sc.parallelize(data,4);
        JavaRDD<String> distFile = sc.textFile("resources/airports.dat",4);

        //add up sizes of all lines
        distFile.map(s -> s.length()).reduce((a, b) -> a + b);
    }
}
