package org.ergemp.spark.training.rdd.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDExamplefromHDFSFile {
    public static void main(String[] args)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("RDDExamplefromHDFSFile").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> distData = jsc.textFile("hdfs://localhost/mockdata/airports/airports.dat");

        distData.foreach(in -> System.out.println(in));
    }
}
