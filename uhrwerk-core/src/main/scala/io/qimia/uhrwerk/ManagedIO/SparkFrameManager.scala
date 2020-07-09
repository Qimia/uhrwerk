package io.qimia.uhrwerk.ManagedIO
import java.time.LocalDateTime

import io.qimia.uhrwerk.models.config.{Connection, Dependency, Target}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkFrameManager {
  def dateToPostFix(date: LocalDateTime): String = {
    val year = date.getYear
    val month = date.getMonth
    val day = date.getDayOfMonth
    val hour = date.getHour
    s"/year=${year}/month=${year}-${month}/day=${year}-${month}-${day}/hour=${hour}"
  }
}

class SparkFrameManager(sparkSession: SparkSession) extends FrameManager {
  override def loadDFFromLake(conn: Connection, locationInfo: Dependency, startTS: Option[LocalDateTime]): DataFrame = {
    assert(conn.getName == locationInfo.getConnectionName)
    // TODO: Make trailing slashes handling better
    val fullLocation = if(startTS.isDefined) {
      conn.getStartPath + locationInfo.getPath + SparkFrameManager.dateToPostFix(startTS.get)
    } else {
      conn.getStartPath + locationInfo.getPath
    }
    sparkSession.read.parquet(fullLocation)
  }

  override def writeDFToLake(frame: DataFrame, conn: Connection, locationInfo: Target, startTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationInfo.getConnectionName)
    val fullLocation = if(startTS.isDefined) {
      conn.getStartPath + locationInfo.getPath + SparkFrameManager.dateToPostFix(startTS.get)
    } else {
      conn.getStartPath + locationInfo.getPath
    }
    frame.write.parquet(fullLocation)
  }
}
