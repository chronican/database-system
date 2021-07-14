package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
            left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
            right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
            condition: RexNode
          ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  protected var left_table : IndexedSeq[IndexedSeq[Any]] = _
  protected var tmp_key_elems: IndexedSeq[Any] = _
  protected var left_keys : IndexedSeq[IndexedSeq[Any]] = _
  protected var right_keys : IndexedSeq[IndexedSeq[Any]] = _
  protected var right_table : IndexedSeq[IndexedSeq[Any]] = _
  protected var left_hash : mutable.HashMap[IndexedSeq[Any],IndexedSeq[Any]] =mutable.HashMap[IndexedSeq[Any],IndexedSeq[Any]]()
  protected var stored : IndexedSeq[IndexedSeq[Any]] = _
  protected var left_input : IndexedSeq[IndexedSeq[Any]] = _
  protected var right_input : IndexedSeq[IndexedSeq[Any]] = _
  protected var sel_vec:IndexedSeq[Boolean] =IndexedSeq[Boolean]()
  protected var out:IndexedSeq[HomogeneousColumn]=IndexedSeq[HomogeneousColumn]()

  override def execute(): IndexedSeq[HomogeneousColumn] ={
    left_input= left.execute().transpose
    if(left_input.isEmpty){return left.execute()}
    right_input = right.execute().transpose
    if(right_input.isEmpty){return right.execute()}

    build_left()
    match_right()

    stored = IndexedSeq[Column]()
    stored = left_keys.indices.filter(i => left_hash.contains(left_keys(i)) && left_hash(left_keys(i)).nonEmpty).flatMap {
      i =>
        left_hash(left_keys(i)).map {
          j => left_table (i) ++ j.asInstanceOf[IndexedSeq[Column]]
        }
    }
    sel_vec=IndexedSeq.fill[Boolean](stored.length)(elem=true)
    stored=stored.transpose:+sel_vec

    for (i <- stored.indices) {
      val ins = toHomogeneousColumn(stored(i))
      out = out :+ ins
    }
    out
  }

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

  def getKey(Keys: IndexedSeq[Int], t: Tuple): IndexedSeq[Any] = {
    Keys.map(i => t(i))
  }

  def build_left(): Unit = {
    left_table=Prepare_table(left_input.transpose)
    left_keys = IndexedSeq[IndexedSeq[Any]]()
    left_keys =left_keys ++(left_table.indices).map(f=>getKey(getLeftKeys, left_table(f)))
    left_keys.indices.filter(i => !left_hash.contains(left_keys(i))).foreach(f => left_hash.update(left_keys(f), IndexedSeq[Any]()))
  }
  def match_right(): Unit = {
    right_table=Prepare_table(right_input.transpose)
    right_keys = IndexedSeq[IndexedSeq[Any]]()
    for (i <- right_table.indices)
    {
      val tmp_key_elems = getKey(getRightKeys,right_table(i))
      if (left_hash.contains(tmp_key_elems))
      {
        left_hash.update(tmp_key_elems, left_hash(tmp_key_elems) :+right_table(i))
      }
    }
  }
}
