package org.ergemp.training.spark.rdd.actions;

public class ForeachPartitionUpsertToDBPseudoCode {
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
