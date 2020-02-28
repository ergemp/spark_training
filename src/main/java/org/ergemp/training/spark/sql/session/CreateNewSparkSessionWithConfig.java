package org.ergemp.training.spark.sql.session;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class CreateNewSparkSessionWithConfig {
    public static void main(String[] args){

        SparkConf conf = new SparkConf()
            .setAppName("CreateNewSparkSessionWithConfig")
            .setMaster("local[*]");

        SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

    }
}
