package org.ergemp.training.spark.sql.session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class NewSessionFromExistingContext {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                                .setAppName("NewSessionFromExistingContext")
                                .setMaster("local[1]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        SparkSession sparkSession = new SparkSession(jsc.sc());

        sparkSession.sql("");
    }
}





