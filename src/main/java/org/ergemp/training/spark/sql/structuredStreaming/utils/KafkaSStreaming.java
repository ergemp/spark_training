package org.ergemp.training.spark.sql.structuredStreaming.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KafkaSStreaming {

    public String bootstrap_servers = "";
    public String topics = "";
    public String app_name = "";
    public String auto_offset_reset = "";

    public Dataset<Row> df;
    public SparkSession spark;

    public Dataset<Row> getDataFrame() {
        this.spark = SparkSession
                .builder()
                .appName(app_name)
                .master("local[2]")
                .getOrCreate();

        this.df = spark
                .readStream()
                .format("kafka")
                //.format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
                .option("kafka.bootstrap.servers", bootstrap_servers)
                .option("subscribe", topics)
                .option("group.id", app_name)
                .option("auto.offset.reset", auto_offset_reset)
                .option("key.deserializer", "StringDeserializer.class")
                .option("value.deserializer", "StringDeserializer.class")
                //.option("startingOffsets", "earliest")
                //.option("endingOffsets", "latest")
                .load();

        //this.df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        return this.df;
    }
}
