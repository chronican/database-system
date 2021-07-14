package ch.epfl.dias.cs422.rel.early.volcano.rle

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilRLEentry, NilTuple, RLEentry, Tuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Decode]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator]]
  */
class Decode protected (
                         input: ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
                       ) extends skeleton.Decode[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
  ch.epfl.dias.cs422.helpers.rel.early.volcano.rle.Operator
](input)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var idx:Int = 0
  private var input_elem: Option[RLEentry] = NilRLEentry
  /**
    * @inheritdoc
    */
  override def open(): Unit = input.open()

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if(idx!=0){
      idx-=1
      Option(input_elem.get.value)
    }
    else{
      input_elem = input.next()
      if (input_elem != NilRLEentry) {
        idx = input_elem.get.length.toInt - 1
        Option(input_elem.get.value)
      }
      else {
        NilTuple
      }
    }
  }


  /**
    * @inheritdoc
    */
  override def close(): Unit = input.close()
}