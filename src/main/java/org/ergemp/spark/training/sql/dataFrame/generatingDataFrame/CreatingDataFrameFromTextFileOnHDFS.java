package org.ergemp.spark.training.sql.dataFrame.generatingDataFrame;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class CreatingDataFrameFromTextFileOnHDFS {
    public static void main(String[] args) {
        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreatingDataFrameFromTextFileOnHDFS")
                .getOrCreate();

        JavaRDD<String> lines = null;
        lines = spark.read().textFile("hdfs://12.0.7.71:8020/delphoi/delphoi-events-json/NewSession/year=2018/month=09/day=25/hour=15").toJavaRDD();
    }
}
