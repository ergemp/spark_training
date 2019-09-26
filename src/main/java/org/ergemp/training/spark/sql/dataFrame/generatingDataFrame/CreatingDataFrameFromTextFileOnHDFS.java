package org.ergemp.training.spark.sql.dataFrame.generatingDataFrame;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class CreatingDataFrameFromTextFileOnHDFS {
    public static void main(String[] args) {
        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreatingDataFrameFromTextFileOnHDFS")
                .master("locals")
                .getOrCreate();

        JavaRDD<String> lines = null;
        lines = spark.read().textFile("hdfs://localhost:8020/delphoi/delphoi-events-json/NewSession/year=2018/month=09/day=25/hour=15").toJavaRDD();
    }
}
