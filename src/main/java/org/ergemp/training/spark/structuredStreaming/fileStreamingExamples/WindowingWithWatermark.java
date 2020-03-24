package org.ergemp.training.spark.structuredStreaming.fileStreamingExamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

public class WindowingWithWatermark {
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

        StructType timedEvent = new StructType().add("timestamp", "timestamp").add("event", "string");

        Dataset<Row> words = spark
                .readStream()
                .option("sep", ";")
                .schema(timedEvent)
                .csv("resources/windowingExample");

        // Group the data by window and word and compute the count of each group
        Dataset<Row> windowedCounts = words
                .withWatermark("timestamp", "10 minutes")
                .groupBy(
                    functions.window(words.col("timestamp"), "10 minutes", "5 minutes"),
                    words.col("event"))
                .count();
    }
}
