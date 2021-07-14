package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry, Tuple, NilTuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEAggregate protected (
                               input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
                               groupSet: ImmutableBitSet,
                               aggCalls: IndexedSeq[AggregateCall]
                             ) extends skeleton.Aggregate[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
](input, groupSet, aggCalls)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  /**
    * @inheritdoc
    */


  protected var num : Int = 0
  protected var stored: IndexedSeq[(Any, RLEentry)] = IndexedSeq[(Any, RLEentry)]()
  protected var result : IndexedSeq[Tuple] = IndexedSeq[Tuple]()
  protected var flag: Boolean = true
  protected var mode: Boolean =_
  protected val null_seq=IndexedSeq(0)

  override def open(): Unit =
  {
    input.open()
    var new_elem: Option[RLEentry] = input.next()
    val key_elems = groupSet.toArray
    while (new_elem != NilRLEentry) {
      var group_actual_keys : IndexedSeq[Any] = IndexedSeq[Any]()
      group_actual_keys= group_actual_keys ++ key_elems.map(i => new_elem.get.value(key_elems(i)))
      if (group_actual_keys == null) {
        group_actual_keys=null_seq
      }
      stored= stored:+ (group_actual_keys, new_elem.get)
      new_elem = input.next()
    }

    if(!groupSet.isEmpty && aggCalls.isEmpty)
    {
      construct_result(stored.groupBy(t=> t._1),groupSet,mode = true)
    }
    else
    {
      for (agg<- aggCalls)
      {
        construct_result(stored.groupMapReduce(t => t._1)(el=> agg.getArgument(el._2.value, el._2.length))((el1, el2) => agg.reduce(el1,el2)),
          groupSet,
          mode = false)
      }
    }
  }


  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] ={
    var output:  Option[RLEentry] = NilRLEentry
    if(num < result(0).length)
    {
      var out = IndexedSeq[Any]()
      for(i <- result.indices)
      {
        if(i == 0 && !flag) {out=result(0)(num).asInstanceOf[IndexedSeq[Any]]}
        else {out = out :+ result(i)(num)}
      }
      output = Option(RLEentry(num, 1, out))
      num+= 1
    }
    output
  }


  /**
    * @inheritdoc
    */
  override def close(): Unit =
  {
    input.close()
  }
  def construct_result(group_map: Map[Any, Any], groupSet: ImmutableBitSet,mode:Boolean): IndexedSeq[IndexedSeq[Any]] = {
    if (group_map.isEmpty) {
      result = result :+ null_seq
    }
    else{
      if (groupSet.length() != 0 && flag){
        if(mode){result = result :+ group_map.keys.toIndexedSeq}
        else{result = result :+ group_map.keys.toIndexedSeq :+ group_map.values.toIndexedSeq}
        flag = false
      }
      else{
        if(mode){result = result :+ null_seq}
        else{result = result :+ group_map.values.toIndexedSeq}
      }
    }
    result
  }

}