package org.ergemp.training.spark.rdd.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SimpleRDDExample {
    public static void main(String[] args)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("simpleRDDExample").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> distData = sc.parallelize(data);

        //defining the parallel workers is optional
        //JavaRDD<Integer> distData = sc.parallelize(data,2);

        distData.foreach(in -> System.out.println(in));
    }
}
