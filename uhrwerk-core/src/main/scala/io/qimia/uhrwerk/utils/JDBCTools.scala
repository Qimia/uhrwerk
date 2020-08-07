package io.qimia.uhrwerk.utils

import java.sql
import java.sql.DriverManager
import java.time.LocalDateTime

import io.qimia.uhrwerk.config.model.Connection
import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.stringtemplate.v4.ST

object JDBCTools {
  /**
   * Creates a new database using the specified connection and database name.
   *
   * @param connection   JDBC Connection
   * @param databaseName Database name
   */
  def createJDBCDatabase(connection: Connection, databaseName: String): Unit = {
    val url = connection.getJdbcUrl
    val driver = connection.getJdbcDriver
    val username = connection.getUser
    val password = connection.getPass
    try {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      statement.execute(s"CREATE DATABASE ${databaseName}")
      connection.close()
    } catch {
      case e: Exception => println(e.getLocalizedMessage)
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
    val driver = connection.getJdbcDriver
    val username = connection.getUser
    val password = connection.getPass
    Class.forName(driver)
    DriverManager.getConnection(url, username, password)
  }

  /**
   * Removes a database using the specified connection and database name.
   *
   * @param conn         Connection information
   * @param databaseName Database name
   */
  def dropJDBCDatabase(conn: Connection, databaseName: String): Unit = {
    try {
      val connection = getJDBCConnection(conn)
      val statement = connection.createStatement
      statement.execute(s"DROP DATABASE ${databaseName}")
      connection.close()
    } catch {
      case e: Exception => println(e.getLocalizedMessage)
    }
  }

  /**
   * Creates a query to obtain the min and max value of the specified partition column.
   *
   * @param lowerBound             Optional lower bound
   * @param upperBound             Optional upper bound
   * @param partitionColumn        Partition column, e.g. id
   * @param partitionQueryTemplate The partition query template
   * @return
   */
  def minMaxQuery(lowerBound: Option[LocalDateTime],
                  upperBound: Option[LocalDateTime],
                  partitionColumn: String,
                  partitionQueryTemplate: String): String = {
    val query = new ST(partitionQueryTemplate)
    if (lowerBound.isDefined) {
      query.add("lower_bound", TimeTools.convertTSToString(lowerBound.get))
    }
    if (upperBound.isDefined) {
      query.add("upper_bound", TimeTools.convertTSToString(upperBound.get))
    }
    val partitionQuery = query.render
    s"(SELECT MIN(partition_query_table.$partitionColumn) AS lower_bound, MAX(partition_query_table.$partitionColumn) AS upper_bound " +
      s"FROM ($partitionQuery) AS partition_query_table) AS tmp_table"
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
   * @return Min max values as Long
   */
  def minMaxQueryIds(sparkSessession: SparkSession,
                     connection: Connection,
                     partitionColumn: String,
                     partitionQuery: String,
                     lowerBound: Option[LocalDateTime] = Option.empty,
                     upperBound: Option[LocalDateTime] = Option.empty): (Long, Long) = {
    val partitionQueryFilled = minMaxQuery(lowerBound, upperBound, partitionColumn, partitionQuery)
    val dbConfig: DataFrameReader = getDbConfig(sparkSessession, connection)
      .option("dbtable", partitionQueryFilled)
      .option("numPartitions", 1)
      .option("fetchsize", 1)
    try {
      val row = dbConfig.load().collect()(0)
      if (row.get(0).isInstanceOf[java.lang.Long])
        (row.getLong(0), row.getLong(1))
      else
        (row.getAs[java.math.BigDecimal](0).longValue(), row.getAs[java.math.BigDecimal](1).longValue())
    } catch {
      case _: Exception => {
        throw new Exception("No rows returned in minMaxQueryIds")
      }
    }
  }

  /**
   * Creates a DataFrameReader from the given jdbc connection
   *
   * @param sparkSession Spark session
   * @param connection   Connection information
   * @return DataFrameReader
   */
  def getDbConfig(sparkSession: SparkSession, connection: Connection): DataFrameReader = {
    sparkSession
      .read
      .format("jdbc")
      .option("url", connection.getJdbcUrl)
      .option("driver", connection.getJdbcDriver)
      .option("user", connection.getUser)
      .option("password", connection.getPass)
  }

  /**
   * Creates a query from the template and optional lower and upper bounds.
   *
   * @param queryTemplate Query template
   * @param lowerBound    Optional lower bound
   * @param upperBound    Optional upper bound
   * @return The final query
   */
  def queryTable(queryTemplate: String,
                 lowerBound: Option[LocalDateTime] = Option.empty,
                 upperBound: Option[LocalDateTime] = Option.empty): String = {
    val query = new ST(queryTemplate)
    if (lowerBound.isDefined) {
      query.add("lower_bound", TimeTools.convertTSToString(lowerBound.get))
    }
    if (upperBound.isDefined) {
      query.add("upper_bound", TimeTools.convertTSToString(upperBound.get))
    }
    s"(${query.render}) AS tmp_table"
  }
}
