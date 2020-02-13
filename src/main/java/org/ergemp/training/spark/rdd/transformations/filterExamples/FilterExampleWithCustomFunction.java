package org.ergemp.training.spark.rdd.transformations.filterExamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class FilterExampleWithCustomFunction {
    public static void main(String[] args)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("FilterExample2").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> distData = jsc.textFile("resources/airports.dat");

        distData.filter(in -> in.contains("Istanbul")).foreach(in -> System.out.println(in));

        distData.filter(in -> {
            return in.contains("Istanbul");
            });

        distData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String in) throws Exception {
                return in.contains("Istanbul");
            }
        });

        distData.filter(new customFilter());
    }

    public static class customFilter implements Function<String, Boolean>
    {
        @Override
        public Boolean call(String in) throws Exception {
            return in.contains("Istanbul");
        }
    }
}