package org.ergemp.training.spark.sql.dataFrame.generatingDataFrame;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreatingDataFrameFromCSV {
    public static void main(String[] args) {
        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("ParseAndLoadCSVToDataFrame")
                .master("local[1]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true") //first line in file has headers
                .option("mode", "DROPMALFORMED")
                .load("hdfs:///csv/file/dir/file.csv");

        Dataset<Row> df2 = spark.sql("SELECT * FROM csv.'hdfs:///csv/file/dir/file.csv'");
    }
}
