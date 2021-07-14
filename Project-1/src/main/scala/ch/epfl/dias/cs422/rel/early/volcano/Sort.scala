package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, NilRLEentry, NilTuple, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import scala.collection.immutable.ListMap
import java.util
import scala.jdk.CollectionConverters._
/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */

class Sort protected (
                       input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
                       collation: RelCollation,
                       offset: Option[Int],
                       fetch: Option[Int]
                     ) extends skeleton.Sort[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](input, collation, offset, fetch)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var sorted = List[Tuple]()
  protected var stored =List[Tuple]()
  protected var num : Int = 0
  /**
    * @inheritdoc
    */
  def sortfunc(e1 : Tuple, e2 : Tuple, coll : RelCollation): Boolean = {
    for(i <- 0 until coll.getFieldCollations.size())
    {
      var comp1 = e1(coll.getFieldCollations.get(i).getFieldIndex).asInstanceOf[Comparable[Any]]
      var comp2 = e2(coll.getFieldCollations.get(i).getFieldIndex).asInstanceOf[Comparable[Any]]
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
  override def open(): Unit = {
    input.open()
    var new_elem: Option[Tuple] = input.next()
    while(new_elem != NilTuple)
    {
      stored = stored :+ new_elem.get
      new_elem = input.next()
    }
    sorted = stored.sortWith((e1,e2) => sortfunc(e1,e2,collation))
  }


  override def next(): Option[Tuple]= {
    var out : Option[Tuple] = NilTuple
    if ((sorted.length > num))
    {
      out = Option(sorted(num))
    }
    num +=1
    out
  }

  override def close(): Unit = {
    input.close()
  }
}






