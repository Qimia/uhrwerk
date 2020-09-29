package io.qimia.uhrwerk.common.tools

import java.sql.{DriverManager, ResultSet}
import java.time.LocalDateTime
import java.{lang, sql}

import io.qimia.uhrwerk.common.model.Connection
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrameReader, SparkSession}

import scala.io.Source

object JDBCTools {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def addIndexToTable(connection: Connection, tableSchema: String, tableName: String, timeColumnJDBC: String): Unit = {
    try {
      val jdbcConnection = getJDBCConnection(connection)
      val statement = jdbcConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
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
        statement.execute(s"CREATE INDEX $indexName ON `$tableSchema`.`$tableName`($timeColumnJDBC)")
      }
      jdbcConnection.close()
    } catch {
      case e: Exception => logger.warn(e.getLocalizedMessage)
    }
  }

  def executeSqlFile(connection: Connection, fileName: String): Unit = {
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

  /**
   * Creates a new database using the specified connection and database name.
   *
   * @param connection   JDBC Connection
   * @param databaseName Database name
   */
  def createJDBCDatabase(connection: Connection, databaseName: String): Unit = {
    try {
      val jdbcConnection = getJDBCConnection(connection)
      val statement = jdbcConnection.createStatement
      statement.execute(s"CREATE DATABASE $databaseName")
      jdbcConnection.close()
    } catch {
      case e: Exception => logger.warn(e.getLocalizedMessage)
    }
  }

  /**
   * Creates a JDBC connection.
   *
   * @param connection Connection information
   * @return The created SQL connection
   */
  def getJDBCConnection(connection: Connection): sql.Connection = {
    val url = connection.getJdbcUrl
    val username = connection.getJdbcUser
    val password = connection.getJdbcPass
    DriverManager.getConnection(url, username, password)
  }

  /**
   * Removes a database using the specified connection and database name.
   *
   * @param connection   Connection information
   * @param databaseName Database name
   */
  def dropJDBCDatabase(connection: Connection, databaseName: String): Unit = {
    try {
      val jdbcConnection = getJDBCConnection(connection)
      val statement = jdbcConnection.createStatement
      statement.execute(s"DROP DATABASE `$databaseName`")
      jdbcConnection.close()
    } catch {
      case e: Exception => logger.warn(e.getLocalizedMessage)
    }
  }

  /**
   * Fills in query's parameters if defined.
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
        TimeTools.convertTSToUTCString(lowerBound.get)
      )
    }
    if (upperBound.isDefined) {
      query = query.replace(
        "<upper_bound>",
        TimeTools.convertTSToUTCString(upperBound.get)
      )
    }
    if (path.isDefined) {
      query = query.replace("<path>", path.get)
    }
    query.replace(";", "")
  }

  /**
   * Creates a query to obtain the min and max value of the specified partition column.
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
    s"(SELECT MIN(partition_query_table.$partitionColumn) AS min_id, MAX(partition_query_table.$partitionColumn) AS max_id " +
      s"FROM ($query) AS partition_query_table) AS tmp_table"
  }

  /**
   * Returns the min max values of the partition column using the partition query.
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
                      connection: Connection,
                      partitionColumn: String,
                      partitionQuery: String,
                      lowerBound: Option[LocalDateTime] = Option.empty,
                      upperBound: Option[LocalDateTime] = Option.empty,
                      path: Option[String] = Option.empty
                    ): (Long, Long) = {
    val partitionQueryFilled =
      minMaxQuery(lowerBound, upperBound, partitionColumn, partitionQuery, path)
    val dbConfig: DataFrameReader = getDbConfig(sparkSessession, connection)
      .option("dbtable", partitionQueryFilled)
      .option("numPartitions", 1)
      .option("fetchsize", 1)
    try {
      val row = dbConfig.load().collect()(0)
      row.get(0) match {
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

  /**
   * Creates a DataFrameReader from the given jdbc connection
   *
   * @param sparkSession Spark session
   * @param connection   Connection information
   * @return DataFrameReader
   */
  def getDbConfig(
                   sparkSession: SparkSession,
                   connection: Connection
                 ): DataFrameReader = {
    sparkSession.read
      .format("jdbc")
      .option("url", connection.getJdbcUrl)
      .option("driver", connection.getJdbcDriver)
      .option("user", connection.getJdbcUser)
      .option("password", connection.getJdbcPass)
  }

  /**
   * Creates a query from the template and optional lower and upper bounds.
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
    s"($query) AS tmp_table"
  }
}
