package io.qimia.uhrwerk.common.tools

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import java.sql.{DriverManager, ResultSet}
import java.time.LocalDateTime
import java.{lang, sql}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrameReader, SparkSession}

import scala.io.Source

object JDBCTools {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def addIndexToTable(
      connection: ConnectionModel,
      tableSchema: String,
      tableName: String,
      timeColumnJDBC: String
  ): Unit = {
    try {
      val jdbcConnection = getJDBCConnection(connection)
      val statement = jdbcConnection.createStatement(
        ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY
      )
      val indexName = s"index_${tableSchema}_${tableName}_$timeColumnJDBC"

      // todo add support for dbs other than mysql
      val isResult = statement.execute(
        s"SELECT * FROM " +
          s"(SELECT COUNT(1) IndexIsThere FROM INFORMATION_SCHEMA.STATISTICS " +
          s"WHERE table_schema='$tableSchema' AND table_name='$tableName' " +
          s"AND index_name='$indexName') t " +
          s"WHERE IndexIsThere > 0"
      )
      val isIndexThere = if (isResult) {
        statement.getResultSet.first()
      } else {
        false
      }
      if (!isIndexThere) {
        statement.execute(
          s"CREATE INDEX $indexName ON `$tableSchema`.`$tableName`($timeColumnJDBC)"
        )
      }
      jdbcConnection.close()
    } catch {
      case e: Exception => logger.warn(e.getLocalizedMessage)
    }
  }

  def executeSqlFile(connection: ConnectionModel, fileName: String): Unit = {
    val jdbcConnection = getJDBCConnection(connection)

    val statement = jdbcConnection.createStatement
    val sqlCommands = Source
      .fromResource(fileName)
      .mkString
      .split(";")
      .map(s => s.trim)
      .filter(s => s.length > 0)
    sqlCommands.foreach(c => {
      statement.execute(c)
    })
    jdbcConnection.close()
  }

  /** Creates a new database using the specified connection and database name.
    *
    * @param connection   JDBC Connection
    * @param databaseName Database name
    */
  def createJDBCDatabase(
      connection: ConnectionModel,
      databaseName: String
  ): Unit = {
    try {
      val jdbcConnection = getJDBCConnection(connection)
      val statement = jdbcConnection.createStatement
      statement.execute(s"CREATE DATABASE $databaseName")
      jdbcConnection.close()
    } catch {
      case e: Exception => logger.warn(e.getLocalizedMessage)
    }
  }

  /** Creates a JDBC connection.
    *
    * @param connection Connection information
    * @return The created SQL connection
    */
  def getJDBCConnection(connection: ConnectionModel): sql.Connection = {
    val url = connection.getJdbcUrl
    val username = connection.getJdbcUser
    val password = connection.getJdbcPass
    DriverManager.getConnection(url, username, password)
  }

  /** Removes a database using the specified connection and database name.
    *
    * @param connection   Connection information
    * @param databaseName Database name
    */
  def dropJDBCDatabase(
      connection: ConnectionModel,
      databaseName: String
  ): Unit = {
    try {
      val jdbcConnection = getJDBCConnection(connection)
      val statement = jdbcConnection.createStatement
      statement.execute(s"DROP DATABASE `$databaseName`")
      jdbcConnection.close()
    } catch {
      case e: Exception => logger.warn(e.getLocalizedMessage)
    }
  }

  /** Fills in query's parameters if defined.
    *
    * @param queryTemplate Query Template.
    * @param lowerBound    Optional lower bound.
    * @param upperBound    Optional upper bound.
    * @param path          Optional path.
    * @return The enriched query.
    */
  def fillInQueryParameters(
      queryTemplate: String,
      lowerBound: Option[LocalDateTime] = Option.empty,
      upperBound: Option[LocalDateTime] = Option.empty,
      path: Option[String] = Option.empty
  ): String = {
    var query = queryTemplate
    if (lowerBound.isDefined) {
      query = query.replace(
        "<lower_bound>",
        TimeTools.convertTSToString(lowerBound.get)
      )
    }
    if (upperBound.isDefined) {
      query = query.replace(
        "<upper_bound>",
        TimeTools.convertTSToString(upperBound.get)
      )
    }
    if (path.isDefined) {
      query = query.replace("<path>", path.get)
    }
    query.replace(";", "")
  }

  /** Fills in query's parameters if defined.
    *
    * @param queryTemplate   Query Template.
    * @param lastMaxBookmark Optional Last (delta ingestion) Max Bookmark Value.
    * @param path            Optional path.
    * @return The enriched query.
    */
  def fillInQueryBookmarkParameters(
      queryTemplate: String,
      lastMaxBookmark: Option[String] = Option.empty,
      path: Option[String] = Option.empty
  ): String = {
    var query = queryTemplate
    if (lastMaxBookmark.isDefined) {
      query = query.replace(
        "<last_max_bookmark>",
        lastMaxBookmark.get
      )
    }
    if (path.isDefined) {
      query = query.replace("<path>", path.get)
    }
    query.replace(";", "")
  }

  /** Creates a query to obtain the min and max value of the specified partition column.
    *
    * @param lowerBound             Optional lower bound
    * @param upperBound             Optional upper bound
    * @param partitionColumn        Partition column, e.g. id
    * @param partitionQueryTemplate The partition query template
    * @param path                   Optional path with format schema.table
    * @return
    */
  def minMaxQuery(
      lowerBound: Option[LocalDateTime],
      upperBound: Option[LocalDateTime],
      partitionColumn: String,
      partitionQueryTemplate: String,
      path: Option[String] = Option.empty
  ): String = {
    val query =
      fillInQueryParameters(
        partitionQueryTemplate,
        lowerBound,
        upperBound,
        path
      )
    s"SELECT MIN(partition_query_table.$partitionColumn) AS min_id, MAX(partition_query_table.$partitionColumn) AS max_id " +
      s"FROM ($query) partition_query_table"
  }

  /** Creates a query to obtain the min and max value of the specified partition column.
    *
    * @param lowerBound             Optional lower bound
    * @param upperBound             Optional upper bound
    * @param partitionColumn        Partition column, e.g. id
    * @param partitionQueryTemplate The partition query template
    * @param path                   Optional path with format schema.table
    * @return
    */
  def minMaxBookmarkQuery(
      lastMaxBookmark: Option[String],
      deltaColumn: String,
      partitionColumn: String,
      partitionQueryTemplate: String,
      path: Option[String] = Option.empty
  ): String = {
    val query =
      fillInQueryBookmarkParameters(
        partitionQueryTemplate,
        lastMaxBookmark,
        path
      )
    s"SELECT " +
      s"MIN(partition_query_table.$partitionColumn) AS min_id, " +
      s"MAX(partition_query_table.$partitionColumn) AS max_id, " +
      s"MAX(partition_query_table.$deltaColumn) AS max_bookmark " +
      s"FROM($query) partition_query_table " +
      s"WHERE partition_query_table.$deltaColumn > $lastMaxBookmark"
  }

  /** Returns the min max values of the partition column using the partition query.
    *
    * @param sparkSessession Spark Session
    * @param connection      Connection information
    * @param partitionColumn Partition column
    * @param partitionQuery  Partition query
    * @param lowerBound      Optional lower bound
    * @param upperBound      Optional upper bound
    * @param path            Optional path
    * @return Min max values as Long
    */
  def minMaxQueryIds(
      sparkSessession: SparkSession,
      connection: ConnectionModel,
      partitionColumn: String,
      partitionQuery: String,
      lowerBound: Option[LocalDateTime] = Option.empty,
      upperBound: Option[LocalDateTime] = Option.empty,
      path: Option[String] = Option.empty
  ): (Any, Any) = {
    val partitionQueryFilled =
      minMaxQuery(lowerBound, upperBound, partitionColumn, partitionQuery, path)
    logger.info("Parallel-load minMaxQuery: " + partitionQueryFilled)
    val dbConfig: DataFrameReader = getDbConfig(sparkSessession, connection)
      .option("query", partitionQueryFilled)
    try {
      val row = dbConfig.load().collect()(0)
      row.get(0) match {
        case _: java.sql.Timestamp =>
          (row.getTimestamp(0), row.getTimestamp(1))
        case _: lang.Long =>
          (row.getLong(0), row.getLong(1))
        case _: String =>
          (row.getString(0).toLong, row.getString(1).toLong)
        case _ =>
          (
            row.getAs[java.math.BigDecimal](0).longValue(),
            row.getAs[java.math.BigDecimal](1).longValue()
          )
      }
    } catch {
      case e: Exception =>
        throw new Exception(
          "No rows returned in minMaxQueryIds\n" + e.getLocalizedMessage
        )
    }
  }

  /** Creates a DataFrameReader from the given jdbc connection
    *
    * @param sparkSession Spark session
    * @param connection   Connection information
    * @return DataFrameReader
    */
  def getDbConfig(
      sparkSession: SparkSession,
      connection: ConnectionModel
  ): DataFrameReader = {
    sparkSession.read
      .format("jdbc")
      .option("url", connection.getJdbcUrl)
      .option("driver", connection.getJdbcDriver)
      .option("user", connection.getJdbcUser)
      .option("password", connection.getJdbcPass)
  }

  /** Creates a query from the template and optional lower and upper bounds.
    *
    * @param queryTemplate Query template
    * @param lowerBound    Optional lower bound
    * @param upperBound    Optional upper bound
    * @return The final query
    */
  def createSelectQuery(
      queryTemplate: String,
      lowerBound: Option[LocalDateTime] = Option.empty,
      upperBound: Option[LocalDateTime] = Option.empty,
      path: Option[String] = Option.empty
  ): String = {
    val query =
      fillInQueryParameters(queryTemplate, lowerBound, upperBound, path)
    s"($query) tmp_table"
  }

  def createDeltaSelectQuery(
      queryTemplate: String,
      lastMaxBookmark: Option[String] = Option.empty,
      deltaColumn: String,
      path: Option[String] = Option.empty
  ): String = {
    val query =
      fillInQueryBookmarkParameters(queryTemplate, lastMaxBookmark, path)
    s"$query WHERE $deltaColumn > $lastMaxBookmark"
  }
}
