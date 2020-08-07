package io.qimia.uhrwerk.ManagedIO

import java.time.LocalDateTime

import io.qimia.uhrwerk.config.model.{Connection, Dependency, Source, Table, TableInput, Target}
import org.apache.spark.sql.DataFrame

// Trait that describes how to read and write dataframes to and from the datalake/datawarehouse
trait FrameManager {
  // There could be a spark / iceberg / deltalake / hudi version depending on what the user wants to use

  def loadDataFrame[T <: TableInput](conn: Connection, locationInfo: T, batchTS: Option[LocalDateTime] = Option.empty): DataFrame

  def writeDataFrame(frame: DataFrame, conn: Connection, locationTargetInfo: Target, locationTableInfo: Table,  batchTS: Option[LocalDateTime] = Option.empty): Unit

}
