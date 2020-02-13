package org.ergemp.training.spark.rdd.context.streamingContext;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class CreatingJavaStreamingContext {

    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("creatingJavaStreamingContext");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
    }
}
