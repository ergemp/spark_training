package org.ergemp.training.spark.sql.session.fileIO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadCsvFile {
    public static void main(String[] args){
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Example - Read CSV to RDD")
                .master("local")
                .getOrCreate();

        String csvPath = "resources/nasa-weblogs.txt";
        Dataset<Row> df = spark.read()
                        .format("csv")
                        .option("delimiter","\t")
                        .option("header", "true")
                        .option("mode", "DROPMALFORMED")
                        .load(csvPath);

        df.printSchema();
        df.show(100,false);
    }
}
