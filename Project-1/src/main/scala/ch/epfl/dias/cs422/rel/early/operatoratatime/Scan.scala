package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Scan protected(
                      cluster: RelOptCluster,
                      traitSet: RelTraitSet,
                      table: RelOptTable,
                      tableToStore: ScannableTable => Store
                    ) extends skeleton.Scan[
  ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
](cluster, traitSet, table)
  with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]

  /**
    * @inheritdoc
    */
  protected var i: Int = 0
  protected var stored: ColumnStore = _
  protected var column_num: Int = table.getRowType.getFieldCount
  protected var row_num:Int=table.getRowCount.toInt
  def execute(): IndexedSeq[Column] = {
    var out: IndexedSeq[Column] = IndexedSeq[Column]()
    if (row_num==0) {out}
    else {
      out = out ++ (0 until column_num).map(i => scannable.getColumn(i).toIndexedSeq) :+ IndexedSeq.fill(row_num)(elem = true)
      out
    }
  }
}