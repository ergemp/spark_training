package org.ergemp.spark.training.sql.dataFrame.generatingDataFrame;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreatingDataFrameFromJson {
    public static void main(String[] args){
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("CreatingDataFrameFromJson")
                .master("local[*]")
                .getOrCreate();

        // read json to dataframe
        String jsonPath = "resources/sample.json";
        Dataset<Row> df = spark.read().json(jsonPath);

        df.printSchema();
        df.show(false);
    }
}
