package io.qimia.uhrwerk.ManagedIO
import java.time.LocalDateTime

import io.qimia.uhrwerk.models.config.{Connection, Dependency, Target}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class InMemFrameManager extends FrameManager {
  type partitionedKey = (String, String, LocalDateTime)
  type unpartitionedKey = (String, String)

  val partitionedTables: mutable.Map[partitionedKey, DataFrame] = mutable.HashMap.empty[partitionedKey, DataFrame]
  val unpartitionedTables: mutable.Map[unpartitionedKey, DataFrame] = mutable.HashMap.empty[unpartitionedKey, DataFrame]

  override def loadDataFrame(conn: Connection, locationInfo: Dependency, startTS: Option[LocalDateTime]): DataFrame = {
    assert(conn.getName == locationInfo.getConnectionName)
    if (startTS.isDefined) {
      partitionedTables((conn.getName, locationInfo.getPath, startTS.get))
    } else {
      unpartitionedTables((conn.getName, locationInfo.getPath))
    }
  }

  override def writeDataFrame(frame: DataFrame, conn: Connection, locationInfo: Target, startTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationInfo.getConnectionName)
    if (startTS.isDefined) {
      partitionedTables((conn.getName, locationInfo.getPath, startTS.get)) = frame
    } else {
      unpartitionedTables((conn.getName, locationInfo.getPath)) = frame
    }
  }
}
