package org.ergemp.training.spark.jdbcRDD.utils;

import org.apache.spark.rdd.JdbcRDD;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.sql.ResultSet;

public class MapResult extends AbstractFunction1<ResultSet, Object[]> implements Serializable {
    public Object[] apply(ResultSet row) {
        return JdbcRDD.resultSetToObjectArray(row);
    }
}
