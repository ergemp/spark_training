package org.ergemp.training.spark.sourceExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class S3SourceSQLExample {
    public static void main(String[] args){

        SparkConf conf = new SparkConf();
        conf.setAppName("S3Source").setMaster("local[*]");

        SparkSession spark = SparkSession
                .builder()
                .appName("S3SourceSQL")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .config("spark.hadoop.fs.s3a.access.key", "access.key")
                .config("spark.hadoop.fs.s3a.secret.key", "secret.key")
                .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
                .config("spark.speculation","false")
                .config("spark.driver.allowMultipleContext", "true")
                .config("fs.s3a.fast.upload", "true")
                .getOrCreate();

        Dataset<Row> df = spark.read().text("s3a://bucket_name/");
        df.createOrReplaceTempView("rawEvents");

        Dataset<Row> sqlDF = spark.sql("select count(*) from rawEvents");
        sqlDF.show();

        //read.json
        //start:    19:15:25
        //end:      19:32:44

        //read.text
        //start:    19:41:35
        //end:      19:50:50
    }
}
