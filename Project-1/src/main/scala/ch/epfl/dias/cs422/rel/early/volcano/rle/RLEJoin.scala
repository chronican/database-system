package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, NilTuple, RLEentry, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable
import scala.util.control.Breaks.break

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class RLEJoin(
               left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
               right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
               condition: RexNode
             ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  /**
    * @inheritdoc
    */
  protected var left_input: Option[RLEentry] = _
  protected var right_input: Option[RLEentry] = _
  protected var stored: IndexedSeq[RLEentry] = IndexedSeq[RLEentry]()
  protected var left_hash: mutable.HashMap[IndexedSeq[Any],  IndexedSeq[RLEentry]] = mutable.HashMap[IndexedSeq[Any], IndexedSeq[RLEentry]]()
  protected var left_keys: IndexedSeq[Any] = _
  protected var right_keys: IndexedSeq[Any] = _
  protected var result: IndexedSeq[RLEentry] = _
  protected var ini: Long = 0

  def getKey(t:Tuple,Keys:IndexedSeq[Int]): IndexedSeq[Any] ={Keys.map(i=>t(i))}

  def get_out(input:RLEentry,right:Option[RLEentry],start:Long):RLEentry={
    val out_length=input.length * right.get.length
    val out_value= input.value ++ right.get.value
    val out=RLEentry(start, out_length, out_value)
    out
  }

  override def open(): Unit =
  {
    left.open()
    right.open()
    right_input = right.next()
    left_input = left.next()
    while(left_input != NilRLEentry){
      left_keys = getKey(left_input.get.value, getLeftKeys)
      left_hash.update(left_keys, left_hash.getOrElse(left_keys, IndexedSeq()) :+ left_input.get)
      left_input = left.next()
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] =
  {
    var output: Option[RLEentry] = NilRLEentry
    var flag = true
    while(flag){
      //the rest of the storage is not Empty
      if(!stored.isEmpty){
        flag = false
        output = Option(stored(0))
        stored = stored.drop(1)
      }
      else if(right_input == NilRLEentry){
        flag = false
        output=NilRLEentry
      }
      else if(left_hash.contains(getKey(right_input.get.value,getRightKeys))){
        //introduce new right elems and update the hash table and storage
        flag= false
        val right_keys=getKey(right_input.get.value,getRightKeys)
        result = left_hash(right_keys)
        stored=stored++(1 until result.length).map(i=>get_out(result(i),right_input,ini))
        val out=get_out(result(0),right_input,ini)
        ini = ini + out.length
        output = Option(out)
        right_input = right.next()
      }
      else{
        right_input = right.next()
      }
    }
    output
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit =
  {
    left.close()
    right.close()
    stored=null
    left_hash=null
  }
}
