package io.qimia.uhrwerk.ManagedIO

import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.config.model._
import org.apache.spark.sql.DataFrame

// Trait that describes how to read and write dataframes to and from the datalake/datawarehouse
trait FrameManager {
  // There could be a spark / iceberg / deltalake / hudi version depending on what the user wants to use

  def loadSourceDataFrame(
      conn: Connection,
      locationInfo: Source,
      startTS: Option[LocalDateTime] = Option.empty): DataFrame

  def loadDependencyDataFrame(
      conn: Connection,
      locationInfo: Dependency,
      startTS: Option[LocalDateTime] = Option.empty): DataFrame

  def writeDataFrame(frame: DataFrame,
                     conn: Connection,
                     locationTargetInfo: Target,
                     locationTableInfo: Table,
                     startTS: Option[LocalDateTime] = Option.empty): Unit

  def loadMoreBatches(conn: Connection,
                      locationInfo: Dependency,
                      startTS: LocalDateTime,
                      endTSExcl: LocalDateTime,
                      batchDuration: Duration): DataFrame
}
