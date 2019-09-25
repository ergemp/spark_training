package org.ergemp.spark.training.rdd.context;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class CreatingJavaSparkContextFromStreamingContext {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                                .setAppName("CreatingJavaSparkContextFromStreamingContext")
                                .setMaster("local[2]")
                                .set("spark.driver.allowMultipleContext", "true");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaSparkContext jsc = jssc.sparkContext();
    }
}
