package org.ergemp.training.spark.streaming.socketStreamExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class FilterSocketWriter {
    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setAppName("FilterSocketWriter")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);
        JavaDStream<String> errLines = lines.filter(line -> line.contains("error"));

        errLines.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
