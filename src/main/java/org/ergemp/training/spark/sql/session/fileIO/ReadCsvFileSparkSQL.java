package org.ergemp.training.spark.sql.session.fileIO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadCsvFileSparkSQL {
    public static void main(String[] args){
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Example - Read CSV to RDD")
                .master("local[2]")
                .getOrCreate();


        Dataset<Row> df = spark.sql("select * from csv.`file:///Users/ergemp/IdeaProjects/spark_training/resources/test.csv` " );
        //Dataset<Row> df = spark.sql("select * from csv.`hdfs:///csv/file/dir/file.csv` " );

        df.printSchema();
        df.show(100,false);
    }
}
