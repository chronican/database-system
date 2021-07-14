package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Project protected (
                          input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
                          projects: java.util.List[_ <: RexNode],
                          rowType: RelDataType
                        ) extends skeleton.Project[
  ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
](input, projects, rowType)
  with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple = {
    eval(projects.asScala.toIndexedSeq, input.getRowType)
  }

  protected var table: IndexedSeq[Column] = _
  protected var table_ini: IndexedSeq[Column] = IndexedSeq[Column]()
  var out: IndexedSeq[Column] = IndexedSeq[Column]()

  override def execute(): IndexedSeq[Column] = {
    table = input.execute()
    if (table.isEmpty) {return out}
    val sel_vec = table(table.length - 1)
    table_ini = table.dropRight(1).transpose
    out= out++(table_ini.indices).map(i=> evaluator(table_ini(i)))
    out= out.transpose :+ sel_vec
    out
  }
}




