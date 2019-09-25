package org.ergemp.training.spark.sinkExamples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SaveToDB {
    public static void main(String[] args){
        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("ParseAndLoadCSVToDataFrame")
                .master("local[1]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true") //first line in file has headers
                .option("mode", "DROPMALFORMED")
                .load("resources/test.csv");

        Properties cnnProps = new Properties();
        cnnProps.setProperty("driver", "com.mysql.jdbc.Driver");
        cnnProps.setProperty("user", "postgres");
        cnnProps.setProperty("password", "password");

        df.write().mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://localhost/postgres","spark_table", cnnProps);
        //Dataset<Row> df2 = spark.sql("SELECT * FROM csv.'hdfs:///csv/file/dir/file.csv'");

    }
}
