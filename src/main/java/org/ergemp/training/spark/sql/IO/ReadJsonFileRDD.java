package org.ergemp.training.spark.sql.IO;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadJsonFileRDD {
    public static void main(String[] args) {
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Example - Read JSON to RDD")
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
