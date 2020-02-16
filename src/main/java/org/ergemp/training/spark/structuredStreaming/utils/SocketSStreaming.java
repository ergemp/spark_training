package org.ergemp.training.spark.structuredStreaming.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SocketSStreaming {
    public String app_name = "";
    public Dataset<Row> df;
    public SparkSession spark;

    public Dataset getSocket() {
        spark = SparkSession
                .builder()
                .appName(app_name)
                .master("local[2]")
                .getOrCreate();

        df = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        return df;
    }
}
