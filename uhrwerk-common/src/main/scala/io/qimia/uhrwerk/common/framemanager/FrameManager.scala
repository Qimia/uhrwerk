package io.qimia.uhrwerk.common.framemanager

import io.qimia.uhrwerk.common.metastore.model.{SourceModel, TableModel}
import java.time.LocalDateTime
import org.apache.spark.sql.DataFrame

// Trait that describes how to read and write dataframes to and from the datalake/datawarehouse
trait FrameManager {
  // There could be a spark / iceberg / deltalake / hudi version depending on what the user wants to use

  def loadSourceDataFrame(source: SourceModel,
                          startTS: Option[LocalDateTime] = Option.empty,
                          endTSExcl: Option[LocalDateTime] = Option.empty,
                          dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame

  def loadDependencyDataFrame(dependencyResult: BulkDependencyResult,
                              dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame

  def writeDataFrame(frame: DataFrame,
                     locationTableInfo: TableModel,
                     partitionTS: Array[LocalDateTime],
                     dataFrameWriterOptions: Option[Array[Map[String, String]]] = Option.empty): Unit
}
