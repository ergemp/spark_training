package org.ergemp.training.spark.streaming.windowing;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class ReduceByKeyAndWindowExample {
    public static void main(String[] args){
        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("ReduceByKeyAndWindowExample")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
        jssc.checkpoint("resources/sparkCheckpointDir");

        JavaDStream<String> lines = jssc.socketTextStream("localhost", 19999);

        JavaPairDStream<String, Integer> wordCounts = lines
                .flatMap(line -> Arrays.asList(line.replaceAll("\\s+" , " ").split(" ")).iterator())
                .mapToPair(line -> new Tuple2<String,Integer>(line,1))
                ;
        JavaPairDStream<String, Integer> windowedWordCounts = wordCounts.reduceByKeyAndWindow((i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10));
        windowedWordCounts.print();

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
