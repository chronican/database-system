package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable
import scala.collection.mutable.HashMap
import java.util

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getLeftKeys]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getRightKeys]]
  */
class Join(
            left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
            right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
            condition: RexNode
          ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * @inheritdoc
    */

  protected var num: Int = 0
  protected var left_input: IndexedSeq[IndexedSeq[Any]] = IndexedSeq[IndexedSeq[Any]]()
  protected var left_keys: IndexedSeq[IndexedSeq[Any]] = IndexedSeq[IndexedSeq[Any]]()
  protected var left_hash: mutable.HashMap[IndexedSeq[Any], IndexedSeq[Any]] = mutable.HashMap()
  protected var stored: IndexedSeq[Tuple] = IndexedSeq[Tuple]()

  override def open(): Unit = {
    build_left()
    match_right()
    stored=left_keys.indices.filter(i => left_hash.contains(left_keys(i)) && left_hash(left_keys(i)).nonEmpty).flatMap{
      i=> left_hash(left_keys(i)).map{
        j => left_input(i) ++ j.asInstanceOf[IndexedSeq[Any]]
      }
    }


  }
  override def next(): Option[Tuple] = {
    var out: Option[Tuple] = NilTuple
    if (num < stored.length) {
      out = Option(stored(num))
      num += 1
    }
    out
  }
  override def close(): Unit = {
    left.close()
    right.close()
  }

  def build_left(): Unit = {
    left.open()
    var new_left: Option[Tuple] = left.next()
    while (new_left != NilTuple) {
      left_input = left_input :+ new_left.get
      left_keys = left_keys :+ getKey(getLeftKeys,new_left)
      new_left = left.next()
    }
    left_keys.indices.filter(i => !left_hash.contains(left_keys(i))).foreach(f => left_hash.update(left_keys(f), IndexedSeq[Any]()))
  }

  def match_right(): Unit = {
    right.open()
    var new_right: Option[Tuple] = right.next()
    while (new_right != NilTuple) {
      val tmp_right = getKey(getRightKeys, new_right)
      if (left_hash.contains(tmp_right)) {
        left_hash.update(tmp_right, left_hash(tmp_right) :+ new_right.get)
      }
      new_right = right.next()
    }
  }

  def getKey(Keys:IndexedSeq[Int],t:Option[Tuple]): IndexedSeq[Any] ={Keys.map(i=>t.get(i))}
}







