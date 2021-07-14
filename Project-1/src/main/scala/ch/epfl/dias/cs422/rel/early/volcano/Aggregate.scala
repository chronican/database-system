package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import scala.collection.mutable.ListBuffer



/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  */
class Aggregate protected (
                            input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
                            groupSet: ImmutableBitSet,
                            aggCalls: IndexedSeq[AggregateCall]
                          ) extends skeleton.Aggregate[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](input, groupSet, aggCalls)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected var num: Int = 0
  protected var flag: Boolean = true
  protected var mode: Boolean =_
  protected var stored: List[(Any, IndexedSeq[Any])] = List[(Any, IndexedSeq[Any])]()
  protected var result: IndexedSeq[IndexedSeq[Any]] =IndexedSeq[IndexedSeq[Any]]()
  protected val null_seq=IndexedSeq(0)
  /**
    * @inheritdoc
    */

  override def open(): Unit = {
    input.open()
    var new_elem: Option[Tuple] = input.next()
    val key_elems = groupSet.toArray
    while (new_elem != NilTuple) {
      var group_keys = IndexedSeq[Any]()
      group_keys = group_keys ++ key_elems.map(i => new_elem.get(key_elems(i)))
      if (group_keys == null) {
        group_keys = null_seq
      }
      stored = stored :+ (group_keys, new_elem.get)
      new_elem = input.next()
    }

    if (aggCalls.length == 0) {
      mode=true
      construct_result(stored.groupBy(t => t._1),groupSet,mode)
    }
    else {
      mode=false
      for (agg <- aggCalls) {
        construct_result(stored.groupMapReduce(t => t._1)(element => agg.getArgument(element._2))((e1, e2) => agg.reduce(e1, e2)),groupSet,mode)
      }
    }
  }


  override def next(): Option[Tuple] = {

    var out: Option[Tuple] = NilTuple
    if (num < result(0).length) {
      out = Option(IndexedSeq[Any]())
      for (i <- 0 until result.length) {
        if (i == 0 && !flag) {
          out=Some(result(0)(num).asInstanceOf[IndexedSeq[Any]])
        }
        else {
          out = Some(out.get :+ result(i)(num))
        }
      }
      num += 1
    }
    out
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }

  def construct_result(group_map: Map[Any, Any], groupSet: ImmutableBitSet, mode: Boolean): IndexedSeq[IndexedSeq[Any]] = {
    if (group_map.isEmpty) {
      result = result :+ null_seq
    }
    else {
      if (groupSet.length() != 0 && flag) {
        if (mode) {
          result = result :+ group_map.keys.toIndexedSeq
        }
        else {
          result = result :+ group_map.keys.toIndexedSeq :+ group_map.values.toIndexedSeq
        }
        flag = false
      }
      else {
        if (mode) {
          result = result :+ null_seq
        }
        else {
          result = result :+ group_map.values.toIndexedSeq
        }
      }
    }
    result
  }
}