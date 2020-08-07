package io.qimia.uhrwerk.ManagedIO

import java.time.LocalDateTime

import io.qimia.uhrwerk.models.config.{Connection, Dependency, StepInput, Source, Target}
import org.apache.spark.sql.DataFrame

// Trait that describes how to read and write dataframes to and from the datalake/datawarehouse
trait FrameManager {
  // There could be a spark / iceberg / deltalake / hudi version depending on what the user wants to use

  def loadDataFrame[T <: StepInput](conn: Connection, locationInfo: T, startTS: Option[LocalDateTime] = Option.empty): DataFrame

  def writeDataFrame(frame: DataFrame, conn: Connection, locationInfo: Target, startTS: Option[LocalDateTime] = Option.empty): Unit

}
