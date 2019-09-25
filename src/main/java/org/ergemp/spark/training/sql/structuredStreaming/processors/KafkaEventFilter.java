package org.ergemp.spark.training.sql.structuredStreaming.processors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import training.spark.sql.structuredStreaming.utils.KafkaSStreaming;

public class KafkaEventFilter {
    public static void main (String[] args) {
        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        //create the stream model
        KafkaSStreaming streamJob = new KafkaSStreaming();

        streamJob.bootstrap_servers = "172.31.42.175:9092";
        streamJob.auto_offset_reset = "latest" ;
        streamJob.topics = "xenn_io_events_852";
        streamJob.app_name = "spark-str-streaming-v2";

        //
        //initialize the KafkaSStream
        //
        Dataset<Row> df = streamJob.getDataFrame();

        //
        //define the transformations
        //
        df.printSchema();
    }
}
