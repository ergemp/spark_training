package org.ergemp.training.spark.structuredStreaming.fileStreamingExamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class StreamCSVwithStructType {
    public static void main(String[] args){

        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("structuredStreaming.socketStreamingExamples.StreamCSVwithStructType")
                .getOrCreate();

        // Read all the csv files written atomically in a directory
        StructType userSchema = new StructType().add("name", "string").add("age", "integer");
        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ";")
                .schema(userSchema)
                .csv("resources/fileStreamingCSVFiles");
                //Equivalent to format("csv").load("/path/to/directory")

        csvDF.printSchema();
        System.out.println("is streaming? " + csvDF.isStreaming());
        //csvDF.writeStream().format("console").start();


        //csvDF.writeStream().format("csv").option("path","out/structuredStreaming.socketStreamingExamples.StreamCSVwithStructType.txt");
    }
}
