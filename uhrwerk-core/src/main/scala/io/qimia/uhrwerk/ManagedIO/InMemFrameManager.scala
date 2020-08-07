package io.qimia.uhrwerk.ManagedIO
import java.time.LocalDateTime

import io.qimia.uhrwerk.config.model.{Connection, Dependency, Table, TableInput, Target}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class InMemFrameManager extends FrameManager {
  type partitionedKey = (String, String, LocalDateTime)
  type unpartitionedKey = (String, String)

  val partitionedTables: mutable.Map[partitionedKey, DataFrame] = mutable.HashMap.empty[partitionedKey, DataFrame]
  val unpartitionedTables: mutable.Map[unpartitionedKey, DataFrame] = mutable.HashMap.empty[unpartitionedKey, DataFrame]

  override def loadDataFrame[T <: TableInput](conn: Connection, locationInfo: T, startTS: Option[LocalDateTime]): DataFrame = {
    assert(conn.getName == locationInfo.getConnectionName)
    if (batchTS.isDefined) {
      partitionedTables((conn.getName, locationInfo.getPath, batchTS.get))
    } else {
      unpartitionedTables((conn.getName, locationInfo.getPath))
    }
  }

  override def writeDataFrame(frame: DataFrame, conn: Connection, locationTargetInfo: Target, locationTableInfo: Table, startTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationTargetInfo.getConnectionName)
    if (startTS.isDefined) {
      partitionedTables((conn.getName, locationTargetInfo.getPath, startTS.get)) = frame
    } else {
      unpartitionedTables((conn.getName, locationTargetInfo.getPath)) = frame
    }
  }
}
