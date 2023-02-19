package io.qimia.uhrwerk.common.framemanager

import io.qimia.uhrwerk.common.metastore.model.{SourceModel2, TableModel}

import java.time.LocalDateTime
import org.apache.spark.sql.DataFrame

import java.util.Properties
import scala.collection.mutable

// Trait that describes how to read and write dataframes to and from the datalake/datawarehouse
trait FrameManager {
  // There could be a spark / iceberg / deltalake / hudi version depending on what the user wants to use

  def loadSourceDataFrame(
      source: SourceModel2,
      startTS: Option[LocalDateTime] = Option.empty,
      endTSExcl: Option[LocalDateTime] = Option.empty,
      dataFrameReaderOptions: Option[Map[String, String]] = Option.empty,
      properties: mutable.Map[String, AnyRef] = mutable.HashMap()
  ): DataFrame

  def loadDependencyDataFrame(
      dependencyResult: BulkDependencyResult,
      dataFrameReaderOptions: Option[Map[String, String]] = Option.empty
  ): DataFrame

  def writeDataFrame(
      frame: DataFrame,
      locationTableInfo: TableModel,
      partitionTS: Array[LocalDateTime],
      dataFrameWriterOptions: Option[Array[Map[String, String]]] = Option.empty,
      props: mutable.Map[String, AnyRef] = mutable.Map.empty
  ): Unit
}
