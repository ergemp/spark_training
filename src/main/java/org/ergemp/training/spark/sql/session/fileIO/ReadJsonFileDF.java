package org.ergemp.training.spark.sql.session.fileIO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadJsonFileDF {
    public static void main(String[] args) {
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Example - Read JSON to RDD")
                .master("local[2]")
                .getOrCreate();

        // read list to RDD
        String jsonPath = "resources/sample.json";
        Dataset<Row> df = spark.read().json(jsonPath);

        df.printSchema();
    }
}
