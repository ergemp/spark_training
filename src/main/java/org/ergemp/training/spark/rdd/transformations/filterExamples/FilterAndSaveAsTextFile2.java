package org.ergemp.training.spark.rdd.transformations.filterExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FilterAndSaveAsTextFile2 {
    public static String COMMA_DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("FilterAndSaveAsTextFile2").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports = sc.textFile("resources/airports.dat");

        //JavaRDD<String> airportsUSA = airports.filter(line -> line.contains("United States"));
        JavaRDD<String> airportsUSA = airports.filter(line -> line.split(",")[3].equals("\"United States\""));
        JavaRDD<String> airportsUSANamesandCities = airportsUSA.map(line -> {
            String retVal = line.split(",")[1] + "," + line.split(",")[2] + "," + line.split(",")[3];
            return retVal;
        });

        airportsUSANamesandCities.saveAsTextFile("out/airportsUSANamesandCities.out");
    }
}
