package org.ergemp.spark.training.rdd.context;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class CreatingJavaStreamingContextFromJavaContext {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                                .setAppName("CreatingJavaStreamingContextFromJavaContext")
                                .setMaster("local[1]")
                                .set("spark.driver.allowMultipleContext", "true");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(10));
    }
}
