package org.ergemp.spark.training.sql.dataFrame.generatingDataFrame;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GeneratingRDDFromSparkSession {
    public static void main(String[] args){
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("GeneratingRDDFromSparkSession")
                .master("local[2]")
                .getOrCreate();

        // read list to RDD
        String jsonPath = "resources/sample.json";
        JavaRDD<Row> items = spark.read().json(jsonPath).toJavaRDD();

        items.foreach(item -> {
            System.out.println(item);
        });
    }
}
