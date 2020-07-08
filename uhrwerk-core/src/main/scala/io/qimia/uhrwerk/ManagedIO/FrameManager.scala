package io.qimia.uhrwerk.ManagedIO

import java.time.LocalDateTime

import io.qimia.uhrwerk.models.config.{Connection, Dependency, Target}
import org.apache.spark.sql.DataFrame

// Trait that describes how to read and write dataframes to and from the datalake/datawarehouse
trait FrameManager {
  // There could be a spark / iceberg / deltalake / hudi version depending on what the user wants to use

  def loadDFFromLake(conn: Connection, locationInfo: Dependency, startTS: Option[LocalDateTime] = Option.empty): DataFrame

  def writeDFToLake(frame: DataFrame, conn: Connection, locationInfo: Target, startTS: Option[LocalDateTime] = Option.empty): Unit

  // The generic reading and writing to JDBC / Other data sources is handled by the user for now

}
