package io.qimia.uhrwerk.ManagedIO
import java.time.LocalDateTime

import io.qimia.uhrwerk.config.model.{Connection, Dependency, StepInput, Target}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class InMemFrameManager extends FrameManager {
  type partitionedKey = (String, String, LocalDateTime)
  type unpartitionedKey = (String, String)

  val partitionedTables: mutable.Map[partitionedKey, DataFrame] = mutable.HashMap.empty[partitionedKey, DataFrame]
  val unpartitionedTables: mutable.Map[unpartitionedKey, DataFrame] = mutable.HashMap.empty[unpartitionedKey, DataFrame]

  override def loadDataFrame[T <: StepInput](conn: Connection, locationInfo: T, batchTS: Option[LocalDateTime]): DataFrame = {
    assert(conn.getName == locationInfo.getConnectionName)
    if (batchTS.isDefined) {
      partitionedTables((conn.getName, locationInfo.getPath, batchTS.get))
    } else {
      unpartitionedTables((conn.getName, locationInfo.getPath))
    }
  }

  override def writeDataFrame(frame: DataFrame, conn: Connection, locationInfo: Target, batchTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationInfo.getConnectionName)
    if (batchTS.isDefined) {
      partitionedTables((conn.getName, locationInfo.getPath, batchTS.get)) = frame
    } else {
      unpartitionedTables((conn.getName, locationInfo.getPath)) = frame
    }
  }
}
