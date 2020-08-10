package io.qimia.uhrwerk.utils

import java.time.LocalDateTime

import io.qimia.uhrwerk.config.model.Connection
import io.qimia.uhrwerk.tags.DbTest
import org.junit.jupiter.api.Assertions.assertNotNull
import org.scalatest.flatspec.AnyFlatSpec

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

    val resultAllShouldBe = s"(SELECT MIN(partition_query_table.$partitionColumn) AS min_id, " +
      s"MAX(partition_query_table.$partitionColumn) AS max_id " +
      s"FROM (SELECT id FROM $path WHERE created_at >= '${TimeTools.convertTSToString(lowerBound)}' " +
      s"and created_at < '${TimeTools.convertTSToString(upperBound)}') AS partition_query_table) AS tmp_table"

    val resultOnlyLowerBoundShouldBe = s"(SELECT MIN(partition_query_table.$partitionColumn) AS min_id, " +
      s"MAX(partition_query_table.$partitionColumn) AS max_id " +
      s"FROM (SELECT id FROM $path WHERE created_at = '${TimeTools.convertTSToString(lowerBound)}') " +
      s"AS partition_query_table) AS tmp_table"

    val resultAll = JDBCTools.minMaxQuery(Some(lowerBound), Some(upperBound), partitionColumn, partitionQueryTemplate, Some(path))
    val resultPathExplicit = JDBCTools.minMaxQuery(Some(lowerBound), Some(upperBound), partitionColumn, partitionQueryTemplatePathExplicit)
    val resultOnlyLowerBound = JDBCTools.minMaxQuery(Some(lowerBound), Option.empty, partitionColumn, partitionQueryTemplateOnlyLowerBound, Some(path))

    assert(resultAll === resultAllShouldBe)
    assert(resultPathExplicit === resultAllShouldBe)
    assert(resultOnlyLowerBound === resultOnlyLowerBoundShouldBe)
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
}

