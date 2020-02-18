package org.ergemp.training.spark.streaming.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class ReduceByKey {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                .setAppName("ReduceByKey")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);
        JavaPairDStream<String,Integer> wordCounts = lines
                .flatMap(line -> Arrays.asList(line.replaceAll("\\s+"," ").split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a,b) -> a+b)
                ;

        wordCounts.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
