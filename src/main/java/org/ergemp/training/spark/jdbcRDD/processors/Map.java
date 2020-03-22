package org.ergemp.training.spark.jdbcRDD.processors;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import org.ergemp.training.spark.jdbcRDD.utils.DBConnection;
import org.ergemp.training.spark.jdbcRDD.utils.MapResult;
import scala.reflect.ClassManifestFactory$;

import java.util.List;

public class Map {
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("spark JdbcRDD example").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        DBConnection dbConnection = new DBConnection();
        JdbcRDD<Object[]> jdbcRDD = new JdbcRDD(sc.sc(),
                dbConnection,
                "select * from pg_user where ?=?",
                0L,
                0L ,
                1,
                new MapResult(),
                ClassManifestFactory$.MODULE$.fromClass(Object[].class));

        //JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(Object[].class));
        JavaRDD<Object[]> javaRDD = jdbcRDD.toJavaRDD();

        javaRDD.map(record -> record[0].toString());
        List<String> lines = javaRDD.map(record -> record[0].toString()).collect();
        /*
        List<String> lines = javaRDD.map(new Function<Object[], String>() {
            @Override
            public String call(final Object[] record) throws Exception {
                return record[0].toString();
            }
        }).collect();
        */
        lines.forEach(line -> System.out.println(line));
    }
}
