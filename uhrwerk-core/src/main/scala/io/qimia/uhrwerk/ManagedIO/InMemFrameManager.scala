//package io.qimia.uhrwerk.ManagedIO
//
//import java.nio.file.Paths
//import java.time.{Duration, LocalDateTime}
//
//import io.qimia.uhrwerk.config.model._
//import org.apache.spark.sql.DataFrame
//
//import scala.collection.mutable
//
//class InMemFrameManager extends FrameManager {
//  type partitionedKey = (String, String, LocalDateTime)
//  type unpartitionedKey = (String, String)
//
//  val partitionedTables: mutable.Map[partitionedKey, DataFrame] = mutable.HashMap.empty[partitionedKey, DataFrame]
//  val unpartitionedTables: mutable.Map[unpartitionedKey, DataFrame] = mutable.HashMap.empty[unpartitionedKey, DataFrame]
//
//  override def loadSourceDataFrame(conn: Connection,
//                                   locationInfo: Source,
//                                   startTS: Option[LocalDateTime] = Option.empty,
//                                   dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame = {
//    assert(conn.getName == locationInfo.getConnectionName)
//
//    if (startTS.isDefined) {
//      partitionedTables((conn.getName, locationInfo.getPath, startTS.get))
//    } else {
//      unpartitionedTables((conn.getName, locationInfo.getPath))
//    }
//  }
//
//  override def loadDependencyDataFrame(conn: Connection,
//                                       locationInfo: Dependency,
//                                       startTS: Option[LocalDateTime],
//                                       dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame = {
//    assert(conn.getName == locationInfo.getConnectionName)
//
//    if (startTS.isDefined) {
//      partitionedTables((conn.getName, locationInfo.getPath(true), startTS.get))
//    } else {
//      unpartitionedTables((conn.getName, locationInfo.getPath(true)))
//    }
//  }
//
//  override def writeDataFrame(frame: DataFrame,
//                              conn: Connection,
//                              locationTargetInfo: Target,
//                              locationTableInfo: Table,
//                              startTS: Option[LocalDateTime],
//                              dataFrameWriterOptions: Option[Map[String, String]] = Option.empty): Unit = {
//    assert(conn.getName == locationTargetInfo.getConnectionName)
//
//    val fullPath = Paths.get(locationTableInfo.getPath, "format=", locationTargetInfo.getFormat).toString
//    if (startTS.isDefined) {
//      partitionedTables((conn.getName, fullPath, startTS.get)) = frame
//    } else {
//      unpartitionedTables((conn.getName, fullPath)) = frame
//    }
//  }
//
//  override def loadMoreBatches(conn: Connection, locationInfo: Dependency, startTS: LocalDateTime, endTSExcl: LocalDateTime, batchDuration: Duration): DataFrame = null // todo implement
//
//}
