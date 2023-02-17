package io.qimia.uhrwerk.framemanager.utils

import io.qimia.uhrwerk.common.metastore.model.{
  ConnectionModel,
  ConnectionType,
  DependencyModel,
  PartitionUnit,
  TableModel
}

import java.nio.file.Paths
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import io.qimia.uhrwerk.common.tools.TimeTools
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable

object SparkFrameManagerUtils {
  private val logger: Logger = Logger.getLogger(this.getClass)
  val timeColumns: List[String] =
    List("year", "month", "day", "hour", "minute")
  private[framemanager] val timeColumnsFormats: List[String] =
    List("yyyy", "yyyy-MM", "yyyy-MM-dd", "yyyy-MM-dd-HH", "yyyy-MM-dd-HH-mm")
  private[framemanager] val timeColumnJDBC: String = "uhrwerk_timestamp"

  /** Converts all time columns (based on the partition unit) into strings.
    *
    * @param df DataFrame.
    * @return Converted DataFrame.
    */
  private[framemanager] def convertTimeColumnsToStrings(
      df: DataFrame
  ): DataFrame = {
    timeColumns
      .foldLeft(df)((tmp, timeColumn) =>
        tmp.withColumn(timeColumn, col(timeColumn).cast(StringType))
      )
  }

  /** Concatenates paths into a single string. Handles properly all trailing slashes
    *
    * @param first First path
    * @param more  One or more paths
    * @return Concatenated path
    */
  private[framemanager] def concatenatePaths(
      first: String,
      more: String*
  ): String = {
    if (first.contains("//")) {
      val split = first.split("//")
      return split.head ++ "//" ++ Paths
        .get(split.tail.mkString("//"), more: _*)
        .toString
    }
    Paths.get(first, more: _*).toString
  }

  /** Returns a full location based on the parameters.
    *
    * @param connectionPath Connection path.
    * @param tablePath      Table path.
    * @return Full location.
    */
  private[framemanager] def getFullLocation(
      connectionPath: String,
      tablePath: String,
      partPaths: List[String] = List()
  ): List[String] = {
    if (partPaths.nonEmpty) {
      val paths = partPaths.to[mutable.SortedSet]
      paths
        .map(path => concatenatePaths(connectionPath, tablePath, path))
        .toList
    } else
      List(concatenatePaths(connectionPath, tablePath))
  }

  private[framemanager] def getFullLocationJDBC(
      connectionPath: String,
      tablePath: String
  ): String = {
    if (!isStringEmpty(connectionPath)) {
      connectionPath + tablePath
    } else {
      tablePath
    }
  }

  private[framemanager] def concatenateDateParts(
      first: String,
      second: String
  ): String = {
    first + "-" + second
  }

  private[framemanager] def getTimeValues(
      startTS: LocalDateTime
  ): (String, String, String, String, String) = {
    val startZTS = startTS.atZone(ZoneId.ofOffset("GMT", ZoneOffset.UTC))
    val year = startZTS.getYear.toString
    val month = concatenateDateParts(
      year,
      TimeTools.leftPad(startZTS.getMonthValue.toString)
    )
    val day = concatenateDateParts(
      month,
      TimeTools.leftPad(startZTS.getDayOfMonth.toString)
    )
    val hour =
      concatenateDateParts(day, TimeTools.leftPad(startZTS.getHour.toString))
    val minute =
      concatenateDateParts(hour, TimeTools.leftPad(startZTS.getMinute.toString))

    (year, month, day, hour, minute)
  }

  private[framemanager] def createDatePath(startTS: LocalDateTime): String = {
    val (year, month, day, hour, minute) = getTimeValues(startTS)
    val listOfTimeColumns = List(
      s"year=$year",
      s"month=$month",
      s"day=$day",
      s"hour=$hour",
      s"minute=$minute"
    )

    concatenatePaths(
      listOfTimeColumns.head,
      listOfTimeColumns.slice(1, listOfTimeColumns.length): _*
    )
  }

  /** Concatenates area, vertical, table, version, and format into a path.
    * Either with slashes for a file system or with dashes and a dot for jdbc.
    *
    * @param table      Table.
    * @param fileSystem Whether the path is for a file system or for jdbc.
    * @param format     Target's format.
    * @return The concatenated path.
    */
  private[framemanager] def getTablePath(
      table: TableModel,
      fileSystem: Boolean,
      format: String
  ): String = {
    if (fileSystem) {
      Paths
        .get(
          s"area=${table.getArea}",
          s"vertical=${table.getVertical}",
          s"table=${table.getName}",
          s"version=${table.getVersion}",
          s"format=$format"
        )
        .toString
    } else { // jdbc
      val (tableSchema, tableName) = getJDBCTableSchemaAndName(table)
      "`" + tableSchema + "`.`" + tableName + "`"
    }
  }

  private[framemanager] def getJDBCTableSchemaAndName(
      table: TableModel
  ): (String, String) = {
    (
      table.getArea + "_" + table.getVertical,
      table.getName + "_" + table.getVersion.replace(".", "_")
    )
  }

  /** Concatenates area, vertical, table, version, and format into a path.
    * Either with slashes for a file system or with dashes and a dot for jdbc.
    *
    * @param dependency Dependency.
    * @param fileSystem Whether the path is for a file system or for jdbc.
    * @return The concatenated path.
    */
  private[framemanager] def getDependencyPath(
      dependency: DependencyModel,
      fileSystem: Boolean
  ): String = {
    if (fileSystem) {
      Paths
        .get(
          s"area=${dependency.getArea}",
          s"vertical=${dependency.getVertical}",
          s"table=${dependency.getTableName}",
          s"version=${dependency.getVersion}",
          s"format=${dependency.getFormat}"
        )
        .toString
    } else { // jdbc or redshift
      if (!isStringEmpty(dependency.getTargetDBTableName))
        dependency.getTargetDBTableName
      else
        "`" + dependency.getArea + "_" + dependency.getVertical + "`.`" + dependency.getTableName + "_" + dependency.getVersion
          .replace(".", "_") + "`"
    }
  }

  /** Checks whether a string is empty (or null because of Java classes).
    *
    * @param s String to check.
    * @return True if null or empty.
    */
  private[framemanager] def isStringEmpty(s: String): Boolean = {
    s == null || s.isEmpty
  }

  /** Checks whether a DataFrame contains all time columns.
    *
    * @param df            DataFrame.
    * @param partitionUnit Partition Unit.
    * @return True if the df contains all time columns with proper formatting.
    */
  private[framemanager] def containsTimeColumns(
      df: DataFrame,
      partitionUnit: PartitionUnit
  ): Boolean = {
    if (!timeColumns.forall(df.columns.contains(_))) {
      return false
    }
    df.cache()

    try {
      if (
        timeColumns
          .zip(timeColumnsFormats)
          .forall(p => {
            df.withColumn(
              p._1 + "_transformed",
              to_date(col(p._1).cast(StringType), p._2)
            ).filter(col(p._1 + "_transformed").isNull)
              .count == 0
          })
      ) {
        return true
      }
    } catch {
      case _: Exception => logger.info("The df doesn't contain time columns")
    }
    false
  }

  /** Adds a jdbc time column (timestamp) to a DataFrame from a specified timestamp.
    *
    * @param frame DataFrame to add columns to.
    * @param ts    TimeStamp.
    * @return DataFrame with the time columns.
    */
  private[framemanager] def addJDBCTimeColumn(
      frame: DataFrame,
      ts: LocalDateTime
  ): DataFrame = {
    frame.withColumn(timeColumnJDBC, lit(Timestamp.valueOf(ts)))
  }

  /** Adds a jdbc time column (timestamp) to a DataFrame from its time columns.
    *
    * @param frame          DataFrame to add columns to.
    * @return DataFrame with the time columns.
    */
  private[framemanager] def addJDBCTimeColumnFromTimeColumns(
      frame: DataFrame
  ): DataFrame = {
    frame.withColumn(
      timeColumnJDBC,
      to_timestamp(col(timeColumns.last), timeColumnsFormats.last)
    )
  }

  /** Expands a source's selectColumn (timestamp) into our time columns.
    *
    * @param df           Source DataFrame.
    * @param selectColumn Timestamp column.
    * @return Enriched DataFrame.
    */
  private[framemanager] def addTimeColumnsToDFFromTimestampColumn(
      df: DataFrame,
      selectColumn: String
  ): DataFrame = {
    df.withColumn("year", year(col(selectColumn)))
      .withColumn(
        "month",
        concat(col("year"), lit("-"), leftPad(month(col(selectColumn))))
      )
      .withColumn(
        "day",
        concat(col("month"), lit("-"), leftPad(dayofmonth(col(selectColumn))))
      )
      .withColumn(
        "hour",
        concat(col("day"), lit("-"), leftPad(hour(col(selectColumn))))
      )
      .withColumn(
        "minute",
        concat(col("hour"), lit("-"), leftPad(minute(col(selectColumn))))
      )
  }

  private[framemanager] def leftPad(c: Column): Column = {
    when(length(c) === 1, concat(lit("0"), c)).otherwise(c)
  }

  private[framemanager] def setS3Config(
      sparkSession: SparkSession,
      connection: ConnectionModel
  ): Unit = {
    assert(connection.getType == ConnectionType.S3)
    val config = sparkSession.conf
    if (connection.getPath.contains("s3a")) {
      // s3a
      config.set(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      )
    } else {}
  }
}
