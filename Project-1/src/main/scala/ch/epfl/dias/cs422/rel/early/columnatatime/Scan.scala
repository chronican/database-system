package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Scan protected(
                      cluster: RelOptCluster,
                      traitSet: RelTraitSet,
                      table: RelOptTable,
                      tableToStore: ScannableTable => Store
                    ) extends skeleton.Scan[
  ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
](cluster, traitSet, table)
  with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]

  protected var column_num: Int = table.getRowType.getFieldCount
  protected var row_num:Int=table.getRowCount.toInt
  protected var out: IndexedSeq[HomogeneousColumn] = IndexedSeq[HomogeneousColumn]()
  def execute(): IndexedSeq[HomogeneousColumn] = {
    var out: IndexedSeq[HomogeneousColumn] = IndexedSeq[HomogeneousColumn]()
    if (row_num==0) {out}
    else {
      out = out ++ (0 until column_num).map(i => scannable.getColumn(i)) :+ IndexedSeq.fill(row_num)(elem = true)
      out
    }
  }

}