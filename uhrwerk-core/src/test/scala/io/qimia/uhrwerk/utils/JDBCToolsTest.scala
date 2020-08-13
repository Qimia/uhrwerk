package io.qimia.uhrwerk.utils

import java.time.LocalDateTime

import io.qimia.uhrwerk.config.model.Connection
import io.qimia.uhrwerk.tags.DbTest
import org.junit.jupiter.api.Assertions.assertNotNull
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrameReader, SparkSession}

class JDBCToolsTest extends AnyFlatSpec {
  val DATABASE_NAME = "uhrwerk_jdbc_tools_test"

  "createJDBCDatabase and dropJDBCDatabase" should "create and remove a database from a jdbc connection" taggedAs DbTest in {
    val connection = JDBCToolsTest.getJDBCConnection

    JDBCTools.createJDBCDatabase(connection, DATABASE_NAME)

    JDBCTools.dropJDBCDatabase(connection, DATABASE_NAME) // shouldn't throw an exception

    connection.setUser("nonexistentuser")

    JDBCTools.createJDBCDatabase(connection, DATABASE_NAME) // shouldn't throw an exception

    JDBCTools.dropJDBCDatabase(connection, "nonexistentdbname") // shouldn't throw an exception
  }

  "getJDBCConnection" should "create a jdbc connection" taggedAs DbTest in {
    val connection = JDBCToolsTest.getJDBCConnection

    val jdbcConnection = JDBCTools.getJDBCConnection(connection)

    assertNotNull(jdbcConnection)
  }

  "minMaxQuery" should "create a query to obtain min and max values" taggedAs DbTest in {
    val partitionQueryTemplate = "SELECT id FROM <path> " +
      "WHERE created_at >= '<lower_bound>' and created_at \\< '<upper_bound>'"
    val partitionQueryTemplateOnlyLowerBound = "SELECT id FROM <path> " +
      "WHERE created_at = '<lower_bound>'"
    val partitionQueryTemplatePathExplicit = "SELECT id FROM uhrwerk_minmax_query_test.test_table " +
      "WHERE created_at >= '<lower_bound>' and created_at \\< '<upper_bound>'"
    val partitionColumn = "id"
    val path = "uhrwerk_minmax_query_test.test_table"
    val lowerBound = LocalDateTime.now()
    val upperBound = LocalDateTime.now()

    val partitionQueryTemplateAllExplicit = s"SELECT id FROM ${path} " +
      s"WHERE created_at \\<= '${TimeTools.convertTSToString(upperBound)}'"

    val resultAllShouldBe = s"(SELECT MIN(partition_query_table.$partitionColumn) AS min_id, " +
      s"MAX(partition_query_table.$partitionColumn) AS max_id " +
      s"FROM (SELECT id FROM $path WHERE created_at >= '${TimeTools.convertTSToString(lowerBound)}' " +
      s"and created_at < '${TimeTools.convertTSToString(upperBound)}') AS partition_query_table) AS tmp_table"

    val resultOnlyLowerBoundShouldBe = s"(SELECT MIN(partition_query_table.$partitionColumn) AS min_id, " +
      s"MAX(partition_query_table.$partitionColumn) AS max_id " +
      s"FROM (SELECT id FROM $path WHERE created_at = '${TimeTools.convertTSToString(lowerBound)}') " +
      s"AS partition_query_table) AS tmp_table"

    val resultAllExplicitShouldBe = s"(SELECT MIN(partition_query_table.$partitionColumn) AS min_id, " +
      s"MAX(partition_query_table.$partitionColumn) AS max_id " +
      s"FROM (SELECT id FROM $path WHERE created_at <= '${TimeTools.convertTSToString(upperBound)}'" +
      s") AS partition_query_table) AS tmp_table"

    val resultAll = JDBCTools.minMaxQuery(Some(lowerBound), Some(upperBound), partitionColumn, partitionQueryTemplate, Some(path))
    val resultPathExplicit = JDBCTools.minMaxQuery(Some(lowerBound), Some(upperBound), partitionColumn, partitionQueryTemplatePathExplicit)
    val resultOnlyLowerBound = JDBCTools.minMaxQuery(Some(lowerBound), Option.empty, partitionColumn, partitionQueryTemplateOnlyLowerBound, Some(path))
    val resultAllExplicit = JDBCTools.minMaxQuery(Option.empty, Option.empty, partitionColumn, partitionQueryTemplateAllExplicit)

    assert(resultAll === resultAllShouldBe)
    assert(resultPathExplicit === resultAllShouldBe)
    assert(resultOnlyLowerBound === resultOnlyLowerBoundShouldBe)
    assert(resultAllExplicit === resultAllExplicitShouldBe)
  }

  "executeSqlFile" should "connect to a database and execute an SQL file" taggedAs DbTest in {
    val connection = JDBCToolsTest.getJDBCConnection
    val sqlfile = "testlake/test.sql"
    JDBCTools.executeSqlFile(connection, sqlfile) // shouldn't throw an exception
  }

  "getDbConfig" should "return a DataFrameReader for the Spark Session" taggedAs DbTest in {
    val connection = JDBCToolsTest.getJDBCConnection
    val spark = JDBCToolsTest.getSparkSession
    val reader = JDBCTools.getDbConfig(spark, connection)
    assertNotNull(reader)
    assert(reader.isInstanceOf[DataFrameReader])
  }
  "queryTable" should "should create a temp table query using the upper and lower bounds" taggedAs DbTest in {
    val queryTemplateBoth = "SELECT id FROM uhrwerk_queryTable_test.test_table " +
      "WHERE created_at >= '<lower_bound>' and created_at \\< '<upper_bound>'"
    val queryTemplateLower = "SELECT id FROM uhrwerk_queryTable_test.test_table " +
      "WHERE created_at >= '<lower_bound>'"
    val lowerBound = LocalDateTime.now()
    val upperBound = LocalDateTime.now()
    val queryBothShouldBe = s"(SELECT id FROM uhrwerk_queryTable_test.test_table WHERE created_at >= " +
      s"'${TimeTools.convertTSToString(lowerBound)}' and created_at < '${TimeTools.convertTSToString(upperBound)}') " +
      "AS tmp_table"
    val queryLowerShouldBe = s"(SELECT id FROM uhrwerk_queryTable_test.test_table WHERE created_at >= " +
      s"'${TimeTools.convertTSToString(lowerBound)}') AS tmp_table"
    val resultBoth = JDBCTools.queryTable(queryTemplateBoth, Some(lowerBound), Some(upperBound))
    val resultLower = JDBCTools.queryTable(queryTemplateLower, Some(lowerBound))
    assert(queryBothShouldBe === resultBoth)
    assert(queryLowerShouldBe === resultLower)
  }

}

object JDBCToolsTest {
  def getJDBCConnection: Connection = {
    val conn = new Connection
    conn.setName("testJDBCConnection")
    conn.setType("jdbc")
    conn.setJdbcDriver("com.mysql.cj.jdbc.Driver")
    conn.setJdbcUrl("jdbc:mysql://localhost:53306")
    conn.setUser("root")
    conn.setPass("mysql")
    conn
  }

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName("testing")
      .getOrCreate();
  }
}

