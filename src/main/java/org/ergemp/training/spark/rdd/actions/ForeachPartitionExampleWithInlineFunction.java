package org.ergemp.training.spark.rdd.actions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Iterator;

public class ForeachPartitionExampleWithInlineFunction {
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("ForeachPartitionExampleWithInlineFunction").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> distData = jsc.textFile("resources/airports.dat");

        distData.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> stringIterator) {
                while (stringIterator.hasNext()){
                    String row = stringIterator.next();
                    System.out.println(row);
                }
            }
        });
    }
}
