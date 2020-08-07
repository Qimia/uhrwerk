package io.qimia.uhrwerk.utils

import java.sql
import java.sql.DriverManager

import io.qimia.uhrwerk.config.model.{Connection, Target}

object JDBCTools {
  def createJDBCDatabase(conn: Connection, locationInfo: Target): Unit = {
    val url = conn.getJdbcUrl
    val driver = conn.getJdbcDriver
    val username = conn.getUser
    val password = conn.getPass
    try {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      statement.execute(s"CREATE DATABASE ${conn.getName}") // TODO: This should be set/done differently
      connection.close()
    } catch {
      case e: Exception => println(e.getLocalizedMessage)
    }
  }

  def getJDBCConnection(conn: Connection): sql.Connection = {
    val url = conn.getJdbcUrl
    val driver = conn.getJdbcDriver
    val username = conn.getUser
    val password = conn.getPass
    Class.forName(driver)
    DriverManager.getConnection(url, username, password)
  }

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
}
