package lsh

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
//import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import scala.collection.Searching.InsertionPoint
import java.util.Arrays
import scala.collection
import scala.collection.Searching._


class BaseConstructionBalanced(sqlContext: SQLContext, data: RDD[(String, List[String])], seed : Int, partitions : Int) extends Construction {
  //build buckets here
  val data_hash : MinHash = new MinHash(seed)
  val query_hash : MinHash = new MinHash(seed)
  val max_num=Int.MaxValue
  def computeMinHashHistogram(queries : RDD[(String, Int)]) : Array[(Int, Int)] = {
    val query_res=queries.map(x=>(x._2,x._1)).groupByKey().map(x=>(x._1, x._2.size)).sortByKey().collect()
    query_res
  }
  def computePartitions(histogram : Array[(Int, Int)]) : Array[Int] = {
    //compute the boundaries of bucket partitions
    val bound_size=Math.ceil(histogram.map(x=>x._2).sum.asInstanceOf[Float]/partitions)
    var res = List[Int]()
    var tmp = 0
    for((elem1,elem2)<-histogram){
      tmp+=elem2
      if(tmp>=bound_size){
        res = res :+ (elem1+1)
        tmp=0
      }
    }
    val par= (0 +: res :+max_num).toArray
    par
  }

  override def eval(queries: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    //compute near neighbors with load balancing here
    val query_object_inv =query_hash.execute(queries)
    val data_object = data_hash.execute(data).map(x => (x._2, x._1)).groupByKey().mapValues(v=>v.toSet)
    val range_partition=computePartitions(computeMinHashHistogram(query_object_inv))
    val query_object=query_object_inv.map(x => (x._2, x._1))

    val search_group=(target:Int) => {
      val x = range_partition.toList.search(target) match {
        case Found(a) => a
        case InsertionPoint(a) => Math.max(a - 1, 0)
      }
      x
    }

    val a=query_object.groupBy(x=>search_group(x._1))
    val b=data_object.groupBy(x=>search_group(x._1))
    (0 until partitions).map(i=>((a.filter(x => x._1 == i).flatMap(x => x._2))
      .join(b.filter(x => x._1 == i)
        .flatMap(x => x._2))
      .map(x => (x._2._1, x._2._2))))
      .reduce(_.union(_))
  }
}