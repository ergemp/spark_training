package org.ergemp.spark.training.sql.dataFrame.generatingDataFrame;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class CreatingDataFrameFromTextFile {
    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .appName("CreatingDataFrameFromTextFile")
                .config("spark.some.config.option", "some-value")
                .config("spark.sql.warehouse.dir","/Users/ergemp/Documents/spark-warehouse")
                .config("hive.metastore.warehouse.dir","/Users/ergemp/Documents/hive-warehouse")
                .master("local[*]")
                .getOrCreate();

        Dataset<String> textDF = spark.read().textFile("resources/mock_clickStream.json");
        textDF.printSchema();
        /*
        * root
            |-- value: string (nullable = true)
        * */

        textDF.show();
        /*
        +--------------------+
        |               value|
        +--------------------+
        |{"sid":"2f662532-...|
        |{"sid":"6a5dba20-...|
        |{"sid":"75a86b28-...|
        |{"sid":"dd392bdc-...|
        |{"sid":"10304946-...|
        +--------------------+
        * */

        Long lCount = textDF.count();
        System.out.println(lCount);
        //5




    }
}
