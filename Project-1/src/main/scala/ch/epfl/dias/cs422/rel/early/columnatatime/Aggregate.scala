package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Aggregate protected (
                            input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
                            groupSet: ImmutableBitSet,
                            aggCalls: IndexedSeq[AggregateCall]
                          ) extends skeleton.Aggregate[
  ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
](input, groupSet, aggCalls)
  with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * @inheritdoc
    */
  protected var table_tmp : IndexedSeq[IndexedSeq[Any]] = _
  protected var table : IndexedSeq[IndexedSeq[Any]] = _
  protected var stored: IndexedSeq[(Any, IndexedSeq[Any])] = IndexedSeq[(Any,IndexedSeq[Any])]()
  protected var result : IndexedSeq[IndexedSeq[Any]] = IndexedSeq[IndexedSeq[Any]]()
  protected var out:IndexedSeq[HomogeneousColumn]=IndexedSeq[HomogeneousColumn]()
  protected var flag: Boolean = true
  protected var sel_vec:IndexedSeq[Boolean] =IndexedSeq[Boolean]()
  protected var group_map : Map[Any,Any] = _
  protected var group_key : IndexedSeq[Any] = _
  protected var mode: Boolean =_

  def Prepare_table(input_table:IndexedSeq[IndexedSeq[Any]]):IndexedSeq[IndexedSeq[Any]]={
    var output_table=IndexedSeq[IndexedSeq[Any]]()
    val table_sel=input_table(input_table.length-1)
    var input_tra=input_table.dropRight(1)
    input_tra=input_tra.transpose
    for (i <-input_tra.indices) {
      if (table_sel(i).asInstanceOf[Boolean])
        output_table = output_table:+ input_tra(i)
    }
    output_table
  }
  override def execute(): IndexedSeq[HomogeneousColumn] ={
    table = input.execute().transpose
    if(table.isEmpty){table_tmp=table.transpose}
    else {
      table_tmp=Prepare_table(table.transpose)
    }
    val key_elems = groupSet.toArray
    for (i <- table_tmp.indices)
    {
      var group_actual_keys: IndexedSeq[Any] = IndexedSeq[Any]()
      group_actual_keys=group_actual_keys++key_elems.indices.map(j=> table_tmp(i)(key_elems(j)))
      if(group_actual_keys==null){group_actual_keys=IndexedSeq(0)}
      stored = stored:+ (group_actual_keys, table_tmp(i))
    }

    if(aggCalls.length!=0)
    {
      mode=false
      for (x<- aggCalls)
      {
        group_map = stored.groupMapReduce(t => t._1)(elem => x.getArgument(elem._2))((e1, e2) => x.reduce(e1, e2))
        group_key = group_map.keys.toIndexedSeq
        construct_result(group_map,groupSet,group_key,mode)
      }
    }
    else
    {
      mode=true
      group_map = stored.groupBy(t => t._1)
      group_key = group_map.keys.toIndexedSeq
      construct_result(group_map,groupSet,group_key,mode)
    }


    sel_vec=IndexedSeq.fill[Boolean](result.transpose.length)(elem=true)
    result=result:+sel_vec
    for(i <-result.indices)
    {
      var ins=toHomogeneousColumn(result(i))
      out=out:+ins
    }
    out
  }
  def construct_result(group_map: Map[Any, Any], groupSet: ImmutableBitSet, group_key:IndexedSeq[Any], mode: Boolean): IndexedSeq[IndexedSeq[Any]] = {
    if (group_map.isEmpty) {
      result=result :+ IndexedSeq(0)
    }
    else{
      if(groupSet.length() != 0 && flag){
        result=result++(group_key(0).asInstanceOf[IndexedSeq[Any]].indices).map{
          i=>(group_key.indices).map(j=> group_key(j).asInstanceOf[IndexedSeq[Any]](i))}
        if(mode){result=result}
        else{result = result :+ group_map.values.toIndexedSeq}
        flag = false
      }
      else{
        if(mode){result=result :+ IndexedSeq(0)}
        else{result= result :+ group_map.values.toIndexedSeq}
      }

    }
    result
  }

}

