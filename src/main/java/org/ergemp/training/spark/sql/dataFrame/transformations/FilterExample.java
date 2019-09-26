package org.ergemp.training.spark.sql.dataFrame.transformations;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

public class FilterExample {
    public static void main(String[] args)
    {
        //configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("FilterExample")
                .master("local[1]")
                .getOrCreate();

        JavaRDD<String> lines = null;
        lines = spark.read().textFile("hdfs://localhost:8020//mockdata/flumeData_181230_2000.1546189200983").toJavaRDD();

        Function<String, Boolean> filter = k -> (k.toString().indexOf("cookieGender") >= 0);

        JavaRDD<String> linesFiltered = lines.filter(filter);

        linesFiltered.take(10).forEach(item -> {
            if (item.matches(".*cookieGender.*")) {
                System.out.println(item);
            }
            else {
                System.out.println(item);
                //System.out.println("-----regex doesnt match");
            }
        });
    }
}
