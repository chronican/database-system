package lsh

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction with Serializable {
  //build buckets here
  val query_object=new MinHash(seed)
  val data_object=new MinHash(seed)

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    val queries_sig:RDD[(String, Int)]=query_object.execute(queries)
    val data_sig:RDD[(String, Int)]=data_object.execute(data)
    val queries_new:RDD[(Int, String)] =queries_sig.map(x=>(x._2,x._1))
    val data_new:RDD[(Int, Set[String])]=data_sig.map(x=>(x._2,x._1)).groupByKey().map(x=>(x._1,x._2.toSet))
    val broad: Broadcast[Map[Int, Set[String]]] = SparkContext.getOrCreate().broadcast(data_new.collect().toMap)
    val out:RDD[(String, Set[String])] =queries_new.map(name=>(name._2, broad.value.getOrElse(name._1, Set[String]())))
    out



  }
}
