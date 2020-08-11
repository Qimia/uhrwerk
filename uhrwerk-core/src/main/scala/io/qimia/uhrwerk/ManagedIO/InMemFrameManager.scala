package io.qimia.uhrwerk.ManagedIO

import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.config.model._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class InMemFrameManager extends FrameManager {
  type partitionedKey = (String, String, LocalDateTime)
  type unpartitionedKey = (String, String)

  val partitionedTables: mutable.Map[partitionedKey, DataFrame] = mutable.HashMap.empty[partitionedKey, DataFrame]
  val unpartitionedTables: mutable.Map[unpartitionedKey, DataFrame] = mutable.HashMap.empty[unpartitionedKey, DataFrame]

  override def loadSourceDataFrame(conn: Connection, locationInfo: Source, batchTS: Option[LocalDateTime]): DataFrame = {
    assert(conn.getName == locationInfo.getConnectionName)
    if (batchTS.isDefined) {
      partitionedTables((conn.getName, locationInfo.getFormat, batchTS.get))
    } else {
      unpartitionedTables((conn.getName, locationInfo.getFormat))
    }
  }

  override def loadDependencyDataFrame(conn: Connection, locationInfo: Dependency, batchTS: Option[LocalDateTime]): DataFrame = {
    assert(conn.getName == locationInfo.getConnectionName)
    if (batchTS.isDefined) {
      partitionedTables((conn.getName, locationInfo.getFormat, batchTS.get))
    } else {
      unpartitionedTables((conn.getName, locationInfo.getFormat))
    }
  }

  override def writeDataFrame(frame: DataFrame, conn: Connection, locationTargetInfo: Target, locationTableInfo: Table, startTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationTargetInfo.getConnectionName)
    if (startTS.isDefined) {
      partitionedTables((conn.getName, locationTargetInfo.getFormat, startTS.get)) = frame
    } else {
      unpartitionedTables((conn.getName, locationTargetInfo.getFormat)) = frame
    }
  }

  override def loadMoreBatches(conn: Connection, locationInfo: Dependency, startTS: LocalDateTime, endTSExcl: LocalDateTime, batchDuration: Duration): DataFrame = null // todo implement
}
