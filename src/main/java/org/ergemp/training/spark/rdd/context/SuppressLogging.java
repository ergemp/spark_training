package org.ergemp.training.spark.rdd.context;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SuppressLogging {
    public static void main(String[] args)
    {
        Logger log = Logger.getRootLogger();
        log.setLevel(Level.ERROR);

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("creatingJavaSparkContext").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.setLogLevel("ERROR");
    }
}
