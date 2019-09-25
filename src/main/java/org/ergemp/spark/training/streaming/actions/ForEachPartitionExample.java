package org.ergemp.spark.training.streaming.actions;

public class ForEachPartitionExample {
    public static void main(String[] args){
        /*
        event.map(x => x._2 ).foreachRDD { rdd =>
            rdd.foreachPartition { rddpartition =>
                val thinUrl = "jdbc:phoenix:phoenix.dev:2181:/hbase"
                val conn = DriverManager.getConnection(thinUrl)
                rddpartition.foreach { record =>
                    conn.createStatement().execute("UPSERT INTO myTable VALUES (" + record._1 + ")" )
                }
                conn.commit();
            }
        }
        */
    }
}
