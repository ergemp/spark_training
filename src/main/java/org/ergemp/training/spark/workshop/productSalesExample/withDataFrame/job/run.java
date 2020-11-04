package org.ergemp.training.spark.workshop.productSalesExample.withDataFrame.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class run {
    public static void main(String args[]){

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("SparkJoinsWithDataFrame")
                .getOrCreate();

        Dataset<Row> customers = spark.read().option("header", "true").csv("resources/customerSales/spark_training_customers.csv");
        Dataset<Row> sales = spark.read().option("header", "true").csv("resources/customerSales/spark_training_sales.csv");

        //customers.show(100,false);
        //sales.show(100,false);

        sales.join(customers,"customer_id").groupBy( "city").count().orderBy("city").show();
    }
}
