package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext


class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int) extends Construction {
  // build buckets here
  val query_object=new MinHash(seed)
  val data_object=new MinHash(seed)


  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors here
    val queries_sig:RDD[(String, Int)]=query_object.execute(queries)
    val data_sig:RDD[(String, Int)]=data_object.execute(data)
    val queries_new=queries_sig.map(x=>(x._2,x._1))
    val data_new=data_sig.map(x=>(x._2,x._1))
    val res=queries_new.join(data_new.groupByKey().mapValues(v => v.toSet)).map(x => (x._2._1, x._2._2))
    res
  }
}


