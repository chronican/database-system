package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rex.RexNode
import scala.collection.mutable.HashMap

import scala.collection.mutable

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  protected var left_input: IndexedSeq[Column] = _
  protected var right_input: IndexedSeq[Column] = _
  protected var sel_vec: IndexedSeq[Boolean] = IndexedSeq[Boolean]()
  protected var left_table: IndexedSeq[Column] = _
  protected var right_table: IndexedSeq[Column] = _
  protected var left_keys: IndexedSeq[IndexedSeq[Any]] = _
  protected var right_keys: IndexedSeq[IndexedSeq[Any]] = _
  protected var left_hash: mutable.HashMap[IndexedSeq[Any], IndexedSeq[Any]] = mutable.HashMap[IndexedSeq[Any], IndexedSeq[Any]]()
  protected var stored: IndexedSeq[Column] = _

  override def execute(): IndexedSeq[Column] = {
    left_input = left.execute()
    if (left_input.isEmpty) {return left_input}
    right_input = right.execute()
    if (right_input.isEmpty) {return right_input}

    build_left()
    match_right()

    stored = IndexedSeq[Column]()
    if (left_hash.isEmpty) {
      return stored
    }

    stored = left_keys.indices.filter(i => left_hash.contains(left_keys(i)) && left_hash(left_keys(i)).nonEmpty).flatMap {
      i =>
        left_hash(left_keys(i)).map {
          j => left_table(i) ++ j.asInstanceOf[IndexedSeq[Column]]
        }
    }

    sel_vec = IndexedSeq.fill[Boolean](stored.length)(elem = true)
    stored = stored.transpose :+ sel_vec
    stored
  }

  def getKey(Keys: IndexedSeq[Int], t: Tuple): IndexedSeq[Any] = {
    Keys.map(i => t(i))
  }

  def Prepare_table(input_table:IndexedSeq[Column]):IndexedSeq[Column]={
    var output_table=IndexedSeq[Column]()
    val table_sel=input_table(input_table.length-1)
    var input_tra=input_table.dropRight(1)
    input_tra=input_tra.transpose
    for (i <-input_tra.indices) {
      if (table_sel(i).asInstanceOf[Boolean])
        output_table = output_table:+ input_tra(i)
    }
    output_table
  }


  def build_left(): Unit = {
    left_table=Prepare_table(left_input)
    left_keys = IndexedSeq[IndexedSeq[Any]]()
    left_keys =left_keys ++(left_table.indices).map(f=>getKey(getLeftKeys, left_table(f)))
    left_keys.indices.filter(i => !left_hash.contains(left_keys(i))).foreach(f => left_hash.update(left_keys(f), IndexedSeq[Any]()))
  }

  def match_right(): Unit = {
    right_table=Prepare_table(right_input)
    right_keys = IndexedSeq[IndexedSeq[Any]]()
    for (i <- right_table.indices)
    {
      val tmp_key_elems = getKey(getRightKeys,right_table(i))
      if (left_hash.contains(tmp_key_elems))
      {
        left_hash.update(tmp_key_elems, left_hash(tmp_key_elems) :+ right_table(i))
      }
    }
  }

}
