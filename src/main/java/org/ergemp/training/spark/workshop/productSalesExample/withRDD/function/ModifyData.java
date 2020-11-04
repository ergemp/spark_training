package org.ergemp.training.spark.workshop.productSalesExample.withRDD.function;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import org.apache.spark.api.java.Optional;

import static org.ergemp.training.spark.workshop.productSalesExample.withRDD.function.KEY_VALUE_PARSER.KEY_VALUE_PARSER;

public class ModifyData {
    public static JavaPairRDD<String, String> modify(JavaRDD<Tuple2<Integer, Optional<String>>> d){
        return d.mapToPair(KEY_VALUE_PARSER);
    }
}
