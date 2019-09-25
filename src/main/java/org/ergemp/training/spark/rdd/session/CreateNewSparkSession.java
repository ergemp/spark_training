package org.ergemp.training.spark.rdd.session;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class CreateNewSparkSession {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                .setAppName("CreateNewSparkSession")
                .setMaster("local");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        SparkSession sparkSession2 = SparkSession
                .builder()
                .master("local")
                .appName("CreateNewSparkSession")
                .getOrCreate();

    }
}
