package org.ergemp.training.spark.rdd.context;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class CreatingJavaSparkContext {
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("creatingJavaSparkContext").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
    }
}
