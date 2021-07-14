package lsh

import org.apache.spark.rdd.RDD

class ORConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    var res: RDD[(String, Set[String])] = children(0).eval(rdd)
    (1 until children.length).foreach(i=>res=res.join(children(i).eval(rdd.distinct())).mapValues(x=>x._1 | x._2))
    res
  }
}
