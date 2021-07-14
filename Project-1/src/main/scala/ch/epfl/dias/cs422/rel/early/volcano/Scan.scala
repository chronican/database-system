package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.store.rle.RLEStore
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
                       cluster: RelOptCluster,
                       traitSet: RelTraitSet,
                       table: RelOptTable,
                       tableToStore: ScannableTable => Store
                     ) extends skeleton.Scan[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
](cluster, traitSet, table)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * A [[Store]] is an in-memory storage the data.
    *
    * Accessing the data is store-type specific and thus
    * you have to convert it to one of the subtypes.
    * See [[getRow]] for an example.
    */
  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )
  protected var idx= 0
  protected var table_ini : IndexedSeq[IndexedSeq[Any]] = IndexedSeq[IndexedSeq[Any]]()
  /**
    * Helper function (you do not have to use it or implement it)
    * It's purpose is to show how to convert the [[scannable]] to a
    * specific [[Store]].
    *
    * @param rowId row number (startign from 0)
    * @return the row as a Tuple
    */

  /**
    * @inheritdoc
    */
  private def construct_table(scan_table:Store):IndexedSeq[IndexedSeq[Any]] =
    (0 until table.getRowType.getFieldCount).map{ i =>
      (scan_table.asInstanceOf[RLEStore].getRLEColumn(i)).flatMap{ v =>
        IndexedSeq.fill(v.length.toInt)(v.value)
      }
    }

  private def getRow(rowId: Int): Tuple = scannable match {
    case rleStore: RLEStore if rowId < scannable.getRowCount =>
      /**
        * For this project, it's safe to assume scannable will always
        * be a [[RLEStore]].
        */
      (0 until table.getRowType.getFieldCount).
        flatMap{ i => table_ini(i)(idx).asInstanceOf[Tuple] }
    case _: RLEStore => null
  }


  override def open(): Unit = {
    table_ini=construct_table(scannable)
  }


  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    var out: Option[Tuple] = NilTuple
    if (getRow(idx) != null) {
      out = Option(getRow(idx))
      idx =idx+1
    }
    else {
      out = NilTuple
    }
    out
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    idx = 0
    table_ini = null
  }
}