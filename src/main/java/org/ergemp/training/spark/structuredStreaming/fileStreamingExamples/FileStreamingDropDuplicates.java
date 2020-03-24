package org.ergemp.training.spark.structuredStreaming.fileStreamingExamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class FileStreamingDropDuplicates {
    public static void main(String[] args){

        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("structuredStreaming.socketStreamingExamples.FileStreamingDropDuplicates")
                .getOrCreate();

        StructType timedEvent = new StructType().add("eventTime", "timestamp").add("guid","string").add("event", "string");
        Dataset<Row> streamingDf = spark.readStream()
                .option("sep", ";")
                .schema(timedEvent)
                .csv("resources/fileStreamingDropDuplicates");
                //Equivalent to format("csv").load("/path/to/directory")

        // Without watermark using guid column
        streamingDf.dropDuplicates("guid");

        // With watermark using guid and eventTime columns
        streamingDf
                .withWatermark("eventTime", "10 seconds")
                .dropDuplicates("guid", "eventTime");

    }
}
