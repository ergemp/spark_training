package org.ergemp.training.spark.rdd.actions.samplesOnAirportsData;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class TakeSampleExample {
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("TakeSampleExample").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = jsc.textFile("resources/airports.dat");
        List<String> lCollect = rdd.takeSample(false, 10);

        for (String line : lCollect) {
            System.out.println(line);
        }
    }
}
