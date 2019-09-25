package org.ergemp.spark.training.sql.dataFrame.actions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class foreachPartitionExampleWithLambdaExpression {
    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("CreateNewSparkSession")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().csv("resources/deneme100.csv");

        dataset.foreachPartition(t -> {
            while (t.hasNext()){
                Row row = t.next();
                System.out.println(row.get(0).toString());
            }
        });
    }
}
