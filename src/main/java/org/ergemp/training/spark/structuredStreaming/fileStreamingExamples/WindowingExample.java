package org.ergemp.training.spark.structuredStreaming.fileStreamingExamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import scala.Int;
import shapeless.record;

public class WindowingExample {
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
        Dataset<Row> windowedCounts = words.groupBy(
                //functions.window(words.col("timestamp"), "10 minutes", "5 minutes"),
                functions.window(words.col("timestamp"), "600 minutes", "300 minutes"),
                words.col("event")
        ).count();

        /*
        windowedCounts
                .writeStream()
                .outputMode("append") //can be complete, update, append
                .format("parquet")  //can be parquet, orc, json, csv
                .option("path", "out/fileStreamingExamplesWindowingExample")
                .option("checkpointLocation", "checkpointDir")
                .start()
                ;
        */
        // Exception in thread "main" org.apache.spark.sql.AnalysisException:
        // Append output mode not supported when there are streaming aggregations
        // on streaming DataFrames/DataSets without watermark;

        windowedCounts.writeStream().foreach(new myForEachWriter());

    }

    public static class myForEachWriter extends ForeachWriter {

        @Override
        public boolean open(long l, long l1) {
            return false;
        }

        @Override
        public void process(Object o) {
            Row row = (Row)o;
            System.out.println(row.get(0) + " - " + row.get(1)) ;
        }

        @Override
        public void close(Throwable throwable) {

        }
    }

}
