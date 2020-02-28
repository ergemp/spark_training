package org.ergemp.training.spark.sql.IO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadParquetFile {
    public static void main(String[] args){
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Example - Read JSON to RDD")
                .master("local[2]")
                .getOrCreate();

        String parquetPath = "/Users/ergemp/IdeaProjects/spark_training/resources/part-00017.snappy.parquet" ;
        Dataset<Row> df = spark.read().parquet(parquetPath);

        df.createOrReplaceTempView("clickstream");

        df.printSchema();
        df
                .select("id","name" )
                .where("name != 'visit' ")
                .distinct()
                .show();

        spark
                .sql("select count(1) as total, name from clickstream group by name")
                .show(100, false);
    }
}
