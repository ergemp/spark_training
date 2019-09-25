package org.ergemp.spark.training.rdd.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDExamplefromMultipleTextFile {
    public static void main(String[] args)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("RDDExamplefromTextFile").setMaster("local[1]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> distData = jsc.textFile("resources/airports.dat");

        //Multiple text files
        //JavaRDD<String> distData = jsc.textFile("resources/airports.dat,resources/airports.dat,resources/airports.dat");

        //All files in a folder
        //JavaRDD<String> distData = jsc.textFile("resources");

        //All files in folders
        //JavaRDD<String> distData = jsc.textFile("resources,other-folder-name");

        //All files for a pattern
        //JavaRDD<String> distData = jsc.textFile("data/rdd/input/file[0-3].txt,data/rdd/anotherFolder/file*");

        //distData.foreach(in -> System.out.println(in));

        for (String line : distData.take(10)){
            System.out.println(line);
        }
    }
}
