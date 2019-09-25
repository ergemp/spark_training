package org.ergemp.training.spark.rdd.transformations.pairRDDFunctions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class GroupByKeyExample {
    public static String COMMA_DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GroupByKeyExample").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("resources/airports.dat");

        JavaPairRDD<String, String> countryNameAirport = airports.mapToPair (line -> new Tuple2<>( line.split(COMMA_DELIMITER)[3], line.split(COMMA_DELIMITER)[1] ));
        JavaPairRDD<String, Iterable<String>> groupedCountryNameAirport = countryNameAirport.groupByKey();

        for (Map.Entry<String, Iterable<String>> airport : groupedCountryNameAirport.collectAsMap().entrySet()) {
            System.out.println(airport.getKey() + " : " + airport.getValue());
        }
    }
}
