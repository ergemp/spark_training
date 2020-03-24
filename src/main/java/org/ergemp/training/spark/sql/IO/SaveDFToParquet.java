package org.ergemp.training.spark.sql.IO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SaveDFToParquet {
    public static void main(String[] args){
        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("SaveDFToParquet")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true") //first line in file has headers
                .option("mode", "DROPMALFORMED")
                .load("resources/test.csv");

        df.write().parquet("hdfs://localhost:8020/out/SaveDFToParquetExample");
    }
}
