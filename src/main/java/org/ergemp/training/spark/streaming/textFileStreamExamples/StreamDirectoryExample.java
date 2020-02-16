package org.ergemp.training.spark.streaming.textFileStreamExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamDirectoryExample {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                .setAppName("StreamDirectoryExample")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(
                conf,
                new Duration(10000)); //10 secs

        JavaDStream<String> data = jssc.textFileStream("resources/fileStreamingFiles" );
        data.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
