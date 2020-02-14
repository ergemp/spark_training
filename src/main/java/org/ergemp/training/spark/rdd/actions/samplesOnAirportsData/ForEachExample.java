package org.ergemp.training.spark.rdd.actions.samplesOnAirportsData;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ForEachExample {
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("ForEachExample").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = jsc.textFile("resources/airports.dat");
        rdd.foreach(line -> System.out.println(line));

        /*
        rdd.foreach(line -> {
            for (Integer i=0; i<=10; i++){
                System.out.println(line);
            }
        });
        */
    }
}
