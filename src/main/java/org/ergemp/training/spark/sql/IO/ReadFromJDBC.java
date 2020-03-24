package org.ergemp.training.spark.sql.IO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadFromJDBC {
    public static void main(String[] args){
        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("ReadFromJDBC")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost/postgres")
                .option("dbtable", "pg_user")
                .option("user", "postgres")
                .option("password", "postgres")
                .load();

        jdbcDF.show(100,false);
    }
}
