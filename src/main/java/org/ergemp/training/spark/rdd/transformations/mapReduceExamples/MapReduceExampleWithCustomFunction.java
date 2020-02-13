package org.ergemp.training.spark.rdd.transformations.mapReduceExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class MapReduceExampleWithCustomFunction {
    public static String COMMA_DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("MapReduceExampleWithCustomFunction").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //passing functions instread of lambda expression
        JavaRDD<String> lines2 = sc.textFile("resources/airports.dat");
        JavaRDD<Integer> lineLengths2 = lines2.map(new GetLength());
        int totalLength2 = lineLengths2.reduce(new Sum());

        System.out.println(totalLength2);
    }

    public static class GetLength implements Function<String, Integer> {
        public Integer call(String s) { return s.length(); }
    }
    public static class Sum implements Function2<Integer, Integer, Integer> {
        public Integer call(Integer a, Integer b) { return a + b; }
    }
}
