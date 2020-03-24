package org.ergemp.training.spark.structuredStreaming.joins;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StreamToStaticJoinExample {
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
        Dataset<Row> staticDf = spark.read(). ...;
        Dataset<Row> streamingDf = spark.readStream(). ...;
        streamingDf.join(staticDf, "type");         // inner equi-join with a static DF
        streamingDf.join(staticDf, "type", "right_join");  // right outer join with a static DF
        */
    }
}
