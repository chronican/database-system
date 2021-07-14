package lsh


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.collection.mutable.ListBuffer


object Main {
  def generate(sc : SparkContext, input_file : String, output_file : String, fraction : Double) : Unit = {
    val rdd_corpus = sc
      .textFile(input_file)
      .sample(false, fraction)

    rdd_corpus.coalesce(1).saveAsTextFile(output_file)
  }

  def recall(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val recall_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._1.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_recall = recall_vec._1/recall_vec._2

    avg_recall
  }

  def precision(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    val precision_vec = ground_truth
      .join(lsh_truth)
      .map(x => (x._1, x._2._1.intersect(x._2._2).size, x._2._2.size))
      .map(x => (x._2.toDouble/x._3.toDouble, 1))
      .reduce((x,y) => (x._1+y._1, x._2+y._2))

    val avg_precision = precision_vec._1/precision_vec._2

    avg_precision
  }

  def construction1(SQLContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction1 composition here
    val lsh1 =  new BaseConstruction(SQLContext, rdd_corpus, 11)
    val lsh2 =  new BaseConstruction(SQLContext, rdd_corpus, 12)
    val lsh3 =  new BaseConstruction(SQLContext, rdd_corpus, 13)
    val lsh4 =  new BaseConstruction(SQLContext, rdd_corpus, 14)
    val lsh5 =  new BaseConstruction(SQLContext, rdd_corpus, 15)
    val lsh6 =  new BaseConstruction(SQLContext, rdd_corpus, 16)
    val lsh7 =  new BaseConstruction(SQLContext, rdd_corpus, 17)
    val lsh8 =  new BaseConstruction(SQLContext, rdd_corpus, 18)
    val lsh9 =  new BaseConstruction(SQLContext, rdd_corpus, 19)
    val lsh10 = new BaseConstruction(SQLContext, rdd_corpus, 20)
    val lsh11 = new BaseConstruction(SQLContext, rdd_corpus, 21)

    val lsh_and = new ANDConstruction(List(lsh1, lsh2, lsh3,lsh4, lsh5, lsh6, lsh7,lsh8,lsh9,lsh10,lsh11))
    lsh_and
  }

  def construction2(SQLContext: SQLContext, rdd_corpus : RDD[(String, List[String])]) : Construction = {
    //implement construction2 composition here
    val lsh1 =  new BaseConstruction(SQLContext, rdd_corpus, 11)
    val lsh2 =  new BaseConstruction(SQLContext, rdd_corpus, 12)
    val lsh3 =  new BaseConstruction(SQLContext, rdd_corpus, 13)
    val lsh4 =  new BaseConstruction(SQLContext, rdd_corpus, 14)
    val lsh5 =  new BaseConstruction(SQLContext, rdd_corpus, 15)
    val lsh6 =  new BaseConstruction(SQLContext, rdd_corpus, 16)
    val lsh7 =  new BaseConstruction(SQLContext, rdd_corpus, 17)
    val lsh8 =  new BaseConstruction(SQLContext, rdd_corpus, 18)
    val lsh9 =  new BaseConstruction(SQLContext, rdd_corpus, 19)
    val lsh10 = new BaseConstruction(SQLContext, rdd_corpus, 20)
    val lsh11 = new BaseConstruction(SQLContext, rdd_corpus, 21)


    val lsh_or = new ORConstruction(List(lsh1, lsh2, lsh3,lsh4, lsh5, lsh6, lsh7, lsh8, lsh9, lsh10,lsh11))
    lsh_or
  }

  def compute_jaccard_distance(a: List[String], b: List[String]): Double = {
    val res=1.0-((a.toSet & b.toSet).size.toDouble /(a.toSet | b.toSet).size)
    res
  }



  def compute_distance(gt : RDD[(String, Set[String])],corpus: RDD[(String, List[String])],query: RDD[(String, List[String])]):Double = {
    val data_map = gt.flatMap{x => x._2.map((_,x._1))}.join(corpus).map(r=>(r._2._1,r._2._2)).join(query)
    val dist_map = data_map.map(x => (x._1,(compute_jaccard_distance(x._2._1,x._2._2),1))).reduceByKey((x,y) => (x._1+y._1,x._2 +y._2)).map(x=>(x._1,x._2._1/x._2._2))
    val dist_avg = dist_map.map(x => (x._2,1)).reduce((x,y) => (x._1+y._1,x._2 +y._2))
    val res=dist_avg._1 / dist_avg._2
    res
  }



  def task8_query_dist(sc : SparkContext, sqlContext : SQLContext, filename1:String,filename2:String) : Unit = {
    //val corpus_file = new File(getClass.getResource(filename1).getFile).getPath
    println("test:",filename1,filename2)
    val corpus_file = filename1

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    //val query_file = new File(getClass.getResource(filename2).getFile).getPath
    val query_file = filename2

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
    //.sample(false, 0.1)

    val exact: Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)
    val lsh_base: Construction = new BaseConstruction(sqlContext, rdd_corpus, 42)
    val lsh_broadcast: Construction = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 42)
    val lsh_balanced: Construction = new BaseConstructionBalanced(sqlContext, rdd_corpus,42, 8)
    // exactNN

    //val t1 = System.nanoTime
    val gt = exact.eval(rdd_query)
    //val exe_time_gt = (System.nanoTime - t1) / 1e9d

    //BaseConstruct
    //val t2 = System.nanoTime
    val out_base = lsh_base.eval(rdd_query)
    //val exe_time_base = (System.nanoTime - t2) / 1e9d


    //BaseConstructBroadcast
    //val t4 = System.nanoTime
    val out_broadcast= lsh_broadcast.eval(rdd_query)
    //val exe_time_broadcast= (System.nanoTime - t4) / 1e9d

    //BaseConstructBalanced
    //val t3 = System.nanoTime
    val out_balanced = lsh_balanced.eval(rdd_query)
    //val exe_time_balanced = (System.nanoTime - t3) / 1e9d


    //Compute distance
    val average_distance_gt=compute_distance(gt,rdd_corpus,rdd_query)
    val average_distance_base=compute_distance(out_base,rdd_corpus,rdd_query)
    val average_distance_balanced=compute_distance(out_balanced,rdd_corpus,rdd_query)
    val average_distance_broadcast=compute_distance(out_broadcast,rdd_corpus,rdd_query)

    //Compute accuracy
    val recall_base=recall(gt, out_base)
    val recall_balanced=recall(gt, out_balanced)
    val recall_broadcast=recall(gt, out_broadcast)
    val precision_base=precision(gt, out_base)
    val precision_balanced=precision(gt, out_balanced)
    val precision_broadcast=precision(gt, out_broadcast)

    //print all the results
    println("test:",filename2)
    //println("the execution time of ExactNN:", exe_time_gt)
    //println("the execution time of BaseConstruct:", exe_time_base)
    //println("the execution time of BaseConstructionBalanced:", exe_time_balanced)
    //println("the execution time of BaseConstructionBroadcast", exe_time_broadcast)

    println("the average distance for ExactNN",average_distance_gt)
    println("the average distance for BaseConstruct",average_distance_base)
    println("the average distance for BaseConstructionBalanced",average_distance_balanced)
    println("the average distance for BaseConstructionBroadcast",average_distance_broadcast)

    println("the recall value for BaseConstruct",recall_base)
    println("the recall value for BaseConstructionBalanced",recall_balanced)
    println("the recall value for BaseConstructionBroadcast",recall_broadcast)
    println("the precision value for BaseConstruct",precision_base)
    println("the precision value for BaseConstructionBalanced",precision_balanced)
    println("the precision value for BaseConstructionBroadcast",precision_broadcast)
  }

  def task8_query_time(sc : SparkContext, sqlContext : SQLContext, filename1:String,filename2:String) : Unit = {
    //val corpus_file = new File(getClass.getResource(filename1).getFile).getPath
    val corpus_file = filename1

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
    /**
    val rdd_corpus_fragment = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .repartition(2)
      .cache()
    val rdd_corpus = ((1 to 10).map(x => rdd_corpus_fragment).reduce(_ ++ _)).repartition(2)**/
    //val query_file = new File(getClass.getResource(filename2).getFile).getPath**/
    val query_file = filename2

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
    /**
    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
      .repartition(2)
      .cache()**/

    val a=ListBuffer[Double]()
    val bc=ListBuffer[Double]()
    val bal=ListBuffer[Double]()

    println(rdd_corpus.count() + rdd_query.count())

    val lsh_base: Construction = new BaseConstruction(sqlContext, rdd_corpus, 42)
    val lsh_broadcast: Construction = new BaseConstructionBroadcast(sqlContext, rdd_corpus, 42)
    val lsh_balanced: Construction = new BaseConstructionBalanced(sqlContext, rdd_corpus,42, 2)



    //BaseConstruct
    for (i <- 1 to 10) {

      //BaseConstructBroadcast

      val t1 = System.nanoTime
      val out_base = lsh_base.eval(rdd_query).count()
      val exe_time_base=(System.nanoTime - t1) / 1e9d
      //exe_time_base:+(System.nanoTime - t1) / 1e9d
      a+=exe_time_base


      val t2 = System.nanoTime
      val out_broadcast = lsh_broadcast.eval(rdd_query).count()
      val exe_time_broadcast = (System.nanoTime - t2) / 1e9d
      //exe_time_broadcast:+(System.nanoTime - t2) / 1e9d
      bc+=exe_time_broadcast


      //BaseConstructBalanced
      val t3 = System.nanoTime
      val out_balanced = lsh_balanced.eval(rdd_query).count()
      val exe_time_balanced = (System.nanoTime - t3) / 1e9d
      bal+=exe_time_balanced


      //print all the results
      //println("test:", filename2)
      //print("test times",i)
      //println("the execution time of BaseConstruct:", exe_time_base)
      //println("the execution time of BaseConstructionBroadcast", exe_time_broadcast)
      //println("the execution time of BaseConstructionBalanced:", exe_time_balanced)
    }
    println("test:", filename2)
    println("average of BaseConstruct:", a.sum/a.length)
    println("average of BaseConstructionBroadcast", bc.sum/bc.length)
    println("average time of BaseConstructionBalanced:",bal.sum/bal.length)


  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sc.setLogLevel("OFF")
    //local file query
    //task8_query_dist(sc, sqlContext,"/corpus-1.csv/part-00000","/queries-1-10.csv/part-00000")
    //task8_query_time(sc, sqlContext,"/corpus-1.csv/part-00000","/queries-1-10.csv/part-00000")

    //cluster file query
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2.csv/part-00000")
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2-skew.csv/part-00000")
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000",
    // "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10.csv/part-00000")
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10-skew.csv/part-00000")
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2.csv/part-00000")
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2-skew.csv/part-00000")
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10.csv/part-00000")
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10-skew.csv/part-00000")
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2.csv/part-00000")
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2-skew.csv/part-00000")
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10.csv/part-00000")
    //task8_query_dist(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10-skew.csv/part-00000")
    /**
    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2.csv/part-00000")
    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-2-skew.csv/part-00000")
    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10.csv/part-00000")
    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-1.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-1-10-skew.csv/part-00000")
    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2.csv/part-00000")
    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-2-skew.csv/part-00000")
    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10.csv/part-00000")
    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-10.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-10-10-skew.csv/part-00000")
    //task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-skewplusplus.csv/part-00000",
    //"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-skewplusplus.csv/part-00000")

    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2.csv/part-00000")
    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-2-skew.csv/part-00000")
    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10.csv/part-00000")
    task8_query_time(sc, sqlContext,"hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/corpus-20.csv/part-00000",
      "hdfs://iccluster041.iccluster.epfl.ch:8020/cs422-data/queries-20-10-skew.csv/part-00000")
    **/

  }
}
