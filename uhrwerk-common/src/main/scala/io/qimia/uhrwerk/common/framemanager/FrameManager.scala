package io.qimia.uhrwerk.common.framemanager

import java.time.LocalDateTime

import io.qimia.uhrwerk.common.model.{Source, Table}
import org.apache.spark.sql.DataFrame

// Trait that describes how to read and write dataframes to and from the datalake/datawarehouse
trait FrameManager {
  // There could be a spark / iceberg / deltalake / hudi version depending on what the user wants to use

  def loadSourceDataFrame(source: Source,
                          startTS: Option[LocalDateTime] = Option.empty,
                          endTSExcl: Option[LocalDateTime] = Option.empty,
                          dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame

  def loadDependencyDataFrame(dependencyResult: BulkDependencyResult,
                              dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame

  def writeDataFrame(frame: DataFrame,
                     locationTableInfo: Table,
                     startTS: Option[LocalDateTime] = Option.empty,
                     dataFrameWriterOptions: Option[Map[String, String]] = Option.empty): Unit
}
