package org.ergemp.spark.training.rdd.transformations.pairRDDFunctions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class GroupByKeyExampleWithInlineFunction {

    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("GroupByKeyExampleWithInlineFunction").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> data = Arrays.asList("the quick brown fox, jumped over the lazy dog");

        JavaRDD<String> distData = sc.parallelize(data);
        distData
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(line -> new Tuple2<String, Integer>(line, 1))
                .groupByKey()
                .map(line -> {
                    Integer total = 0;
                    Iterator it = line._2.iterator();
                    while(it.hasNext()){
                        Integer each = (Integer)it.next();
                        total += each;
                    }
                    return new Tuple2<String, Integer>(line._1, total);
                })
                .foreach(line -> System.out.println("key: " + line._1 + " - " + "value: " + line._2 ) )
                ;
    }
}
