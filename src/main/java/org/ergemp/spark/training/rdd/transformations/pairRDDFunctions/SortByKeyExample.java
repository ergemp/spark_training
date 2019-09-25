package org.ergemp.spark.training.rdd.transformations.pairRDDFunctions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SortByKeyExample {
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("SortByKeyExample").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> distData = jsc.textFile("resources/airports.dat");

        distData.mapToPair(in -> new Tuple2<>(in.split(",")[2], 1))
                .reduceByKey((x,y) -> x+y)
                .mapToPair(in -> new Tuple2<>(in._2, in._1))
                .sortByKey(false)
                .take(10)
                .forEach(in -> System.out.println(in));

        //.foreach(in -> System.out.println(in))
        //;
    }
}
