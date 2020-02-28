package org.ergemp.training.spark.sql.session;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class CreateNewSparkSessionWithBuilder {
    public static void main(String[] args){

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("CreateNewSparkSessionWithBuilder")
                .getOrCreate();

    }
}
