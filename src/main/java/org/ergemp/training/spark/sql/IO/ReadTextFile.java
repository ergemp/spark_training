package org.ergemp.training.spark.sql.IO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadTextFile {
    public static void main(String[] args){
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("ReadTextFile")
                .master("local")
                .getOrCreate();

        // read list to RDD
        String filePath = "resources/test.csv";
        Dataset<Row> items = spark.read().text(filePath);

        items.printSchema();
        items.show();
    }
}
