package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, RLEentry}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Reconstruct]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Reconstruct protected (
                              left: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator,
                              right: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
                            ) extends skeleton.Reconstruct[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
](left, right)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator {

  protected var left_next: Option[RLEentry] = NilRLEentry
  protected var right_next: Option[RLEentry] = NilRLEentry
  protected var out: RLEentry =_
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()
    left_next = left.next()
    right_next= right.next()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[RLEentry] =
  {
    if(left_next == NilRLEentry) {return NilRLEentry}
    if(right_next == NilRLEentry) {return NilRLEentry}
    var start = Math.max(left_next.get.startVID, right_next.get.startVID)
    var end = Math.min(left_next.get.endVID, right_next.get.endVID)
    var length=end-start+1
    var value=left_next.get.value ++ right_next.get.value
    while (length.toInt<1) {
      call_next()
      start = Math.max(left_next.get.startVID, right_next.get.startVID)
      end = Math.min(left_next.get.endVID, right_next.get.endVID)
      length=end-start+1
      value=left_next.get.value ++ right_next.get.value
    }
    val out = RLEentry(start, length, value)
    call_next()
    Option(out)
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
  def call_next():Unit={
    if (left_next.get.endVID == right_next.get.endVID)
    {
      left_next= left.next()
      right_next= right.next()
    }
    else if (left_next.get.endVID > right_next.get.endVID)
    {
      right_next = right.next()
    }
    else if (left_next.get.endVID < right_next.get.endVID)
    {
      left_next = left.next()
    }
  }
}
