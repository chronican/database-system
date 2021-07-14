package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {
  def compute_jaccard_similarity(a: List[String], b: List[String]): Double = {
    val res=(a.toSet & b.toSet).size.toDouble /(a.toSet | b.toSet).size.toDouble
    res
  }

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {

    val filter_res = rdd.cartesian(data).map(movie => (movie._1._1, movie._2._1, compute_jaccard_similarity(movie._1._2, movie._2._2))).filter(z => z._3 >= threshold)
    val movie_name = filter_res.map(movie => (movie._1, movie._2)).groupByKey().mapValues(_.toSet)
    movie_name
  }
}


