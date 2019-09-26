package org.ergemp.training.spark.sql.dataFrame.actions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ForeachPartitionExampleWithLambdaExpression {
    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("ForeachPartitionExampleWithLambdaExpression")
                .getOrCreate();

        Dataset<Row> df = spark.read().csv("resources/deneme100.csv");

        df.foreachPartition(t -> {
            while (t.hasNext()){
                Row row = t.next();
                System.out.println(row.get(0).toString());
            }
        });
    }
}
