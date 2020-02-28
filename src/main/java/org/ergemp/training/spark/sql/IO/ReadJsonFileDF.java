package org.ergemp.training.spark.sql.IO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadJsonFileDF {
    public static void main(String[] args) {
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("ReadJsonFileDF")
                .master("local")
                .getOrCreate();

        // read list to RDD
        String jsonPath = "resources/sample.json";
        Dataset<Row> df = spark.read().json(jsonPath);

        df.printSchema();
    }
}
