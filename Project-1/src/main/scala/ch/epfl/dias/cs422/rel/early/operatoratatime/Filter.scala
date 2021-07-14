package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode
import org.apache.commons.io.filefilter.FalseFileFilter

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Filter protected (
                         input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
                         condition: RexNode
                       ) extends skeleton.Filter[
  ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
](input, condition)
  with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }
  protected var table: IndexedSeq[Column] =_
  protected var table_ini: IndexedSeq[Column] = IndexedSeq[Column]()
  override def execute(): IndexedSeq[Column] = {
    table=input.execute()
    table_ini=table.dropRight(1).transpose
    table_ini=table_ini.transpose:+table_ini.indices.map(i=>predicate(table_ini(i)))
    table_ini
  }
}

