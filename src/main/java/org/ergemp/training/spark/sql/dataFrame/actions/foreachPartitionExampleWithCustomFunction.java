package org.ergemp.training.spark.sql.dataFrame.actions;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;

public class foreachPartitionExampleWithCustomFunction {
    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("CreateNewSparkSession")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().csv("resources/deneme100.csv");

        dataset.foreachPartition(new customForEachPartition());
    }

    private static class customForEachPartition implements ForeachPartitionFunction {
        @Override
        public void call(Iterator iterator) throws Exception {
            while (iterator.hasNext()){
                Row row = (Row)iterator.next();
                System.out.println(row.get(0).toString());
            }
        }
    }
}


