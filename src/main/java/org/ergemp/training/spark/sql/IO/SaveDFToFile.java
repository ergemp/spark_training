package org.ergemp.training.spark.sql.IO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SaveDFToFile {
    public static void main(String[] args){
        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("SaveDFToFile")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true") //first line in file has headers
                .option("mode", "DROPMALFORMED")
                .load("resources/test.csv");

        df.write().mode(SaveMode.Overwrite).csv("out/SaveDFToFileExample.csv");
        df.write().mode(SaveMode.Overwrite).json("out/SaveDFToFileExample.json");
    }
}
