package org.ergemp.training.spark.structuredStreaming.joins;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.expr;

public class StreamToStreamJoinExample {
    public static void main(String[] args){

        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("structuredStreaming.socketStreamingExamples.WindowingExample")
                .getOrCreate();


        /*
        Dataset<Row> impressions = spark.readStream(). ...
        Dataset<Row> clicks = spark.readStream(). ...

        // Apply watermarks on event-time columns
        Dataset<Row> impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours");
        Dataset<Row> clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours");

        // Join with event-time constraints
        impressionsWithWatermark.join(
                clicksWithWatermark,
                expr(
                        "clickAdId = impressionAdId AND " +
                                "clickTime >= impressionTime AND " +
                                "clickTime <= impressionTime + interval 1 hour "),
                                 "leftOuter" // can be "inner", "leftOuter", "rightOuter"
        );
        */
    }
}
