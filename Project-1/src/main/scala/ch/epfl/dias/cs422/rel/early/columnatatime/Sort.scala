package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import java.util
import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Sort protected (
                       input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
                       collation: RelCollation,
                       offset: Option[Int],
                       fetch: Option[Int]
                     ) extends skeleton.Sort[
  ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
](input, collation, offset, fetch)
  with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  protected var stored : IndexedSeq[IndexedSeq[Any]] = _
  protected var sorted : IndexedSeq[IndexedSeq[Any]] = _
  protected var result : IndexedSeq[IndexedSeq[Any]] = _
  protected var table:IndexedSeq[Tuple] = _
  protected var out:IndexedSeq[HomogeneousColumn]=IndexedSeq[HomogeneousColumn]()

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

  override def execute(): IndexedSeq[HomogeneousColumn] = {
    table = input.execute().transpose
    stored = table.transpose.dropRight(1)
    stored = stored.transpose
    sorted = stored.sortWith((elem1, elem2) => sortfunc(elem1, elem2, collation))
    result = IndexedSeq[IndexedSeq[Any]]()
    result=result++(stored.indices).map(i=>sorted(i))
    result = result.transpose:+IndexedSeq.fill[Boolean](result.length)(elem = true)
    out=out++(result.indices).map(f=> toHomogeneousColumn(result(f)))
    out
  }
}
