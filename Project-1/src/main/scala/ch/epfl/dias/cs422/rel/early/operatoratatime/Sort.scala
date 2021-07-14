package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import java.util
import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Sort protected (
                       input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
                       collation: RelCollation,
                       offset: Option[Int],
                       fetch: Option[Int]
                     ) extends skeleton.Sort[
  ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
](input, collation, offset, fetch)
  with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  protected var stored : IndexedSeq[Column] = _
  protected var sorted : IndexedSeq[IndexedSeq[Any]] = _
  protected var result : IndexedSeq[IndexedSeq[Any]] = IndexedSeq[Column]()
  protected var sel_vec:IndexedSeq[Boolean] =IndexedSeq[Boolean]()
  protected var table:IndexedSeq[Column] =_

  def sortfunc(e1 : Tuple, e2 : Tuple, coll : RelCollation): Boolean = {
    for(i <- 0 until coll.getFieldCollations.size())
    {
      val comp1 = e1(coll.getFieldCollations.get(i).getFieldIndex).asInstanceOf[Comparable[Any]]
      val comp2 = e2(coll.getFieldCollations.get(i).getFieldIndex).asInstanceOf[Comparable[Any]]
      if(comp1.compareTo(comp2) != 0)
      {
        if(coll.getFieldCollations.get(i).getDirection.isDescending)
        {
          if(comp1.compareTo(comp2) >0){
            return true
          }
          else{return false}
        }
        else
        {
          if(comp1.compareTo(comp2) >0){
            return false
          }
          else{return true}
        }
      }
    }
    false
  }

  override def execute(): IndexedSeq[Column] = {
    table = input.execute()
    stored=table.dropRight(1).transpose
    sorted = stored.sortWith((elem1,elem2) => sortfunc(elem1,elem2,collation))
    result=result++(stored.indices).map(i=>sorted(i))
    sel_vec=IndexedSeq.fill[Boolean](result.length)(elem=true)
    result=result.transpose:+sel_vec
    result
  }
}
