package io.qimia.uhrwerk.ManagedIO
import java.time.LocalDateTime

import io.qimia.uhrwerk.models.config.{Connection, Dependency, Target}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkFrameManager(sparkSession: SparkSession) extends FrameManager {
  override def loadDFFromLake(conn: Connection, locationInfo: Dependency, startTS: Option[LocalDateTime]): DataFrame = ???

  override def writeDFToLake(frame: DataFrame, conn: Connection, locationInfo: Target, startTS: Option[LocalDateTime]): Unit = ???
}
