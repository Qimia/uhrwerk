package io.qimia.uhrwerk.utils

import java.io.{FileInputStream, FileNotFoundException, IOException}
import java.nio.file.Path

import io.qimia.uhrwerk.config.representation.{Complete, Global, Table}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.representer.Representer

import scala.io.{BufferedSource, Source}


object ConfigReader {

  /**
    * Read a complete dag yaml file
    * @param path path to the yaml file
    * @return the complete dag object
    */
  def readComplete(path: Path): Complete = {
    val fileStream   = new FileInputStream(path.toFile)
    val representer = new Representer
    representer.getPropertyUtils.setSkipMissingProperties(false)
    val yaml = new Yaml(new Constructor(classOf[Complete]), representer)
    val config = yaml.load(fileStream).asInstanceOf[Complete]
    if (config.tablesSet()) {
      val tables = config.getTables
      tables.foreach(t =>
      if (t.sourcesSet()) {
        setQueryStrings(t, path)
      }
      )
    }
    config
  }


  /**
   * Read a global-configuration yaml file
   * @param path path to the yaml file
   * @return the global-config object
   */
  def readGlobalConfig(path: Path): Global = {
    val fileStream   = new FileInputStream(path.toFile)
    val representer = new Representer
    representer.getPropertyUtils.setSkipMissingProperties(false)
    val yaml   = new Yaml(new Constructor(classOf[Global]), representer)
    val config = yaml.load(fileStream).asInstanceOf[Global]
    config
  }

  /**
   * Read a single step-configuration yaml file
   * @param path path to the yaml file
   * @return the step-config object
   */
  def readStepConfig(path: Path): Table = {
    val fileStream   = new FileInputStream(path.toFile)
    val representer = new Representer
    representer.getPropertyUtils.setSkipMissingProperties(false)
    val yaml   = new Yaml(new Constructor(classOf[Table]), representer)
    val config = yaml.load(fileStream).asInstanceOf[Table]
    if (config.sourcesSet()) {
      setQueryStrings(config, path)
    }
    config
  }


  /**
   * Read a query string from an accompanying sql-file
   * @param path String with the path to the sql-file
   * @return String with the sql query
   */
  def readQueryFile(path: String): String = {
    var query = "FILEQUERY FAILED"
    var bufferedSource: BufferedSource = null
    try {
      bufferedSource = Source.fromFile(path)
      query = bufferedSource.getLines.mkString(" ")
    } catch {
      case _: FileNotFoundException => System.err.println(s"Couldn't find query file ${path}.")
      case _: IOException => System.err.println("Had an IOException trying to read query file")
      case e: Throwable => e.printStackTrace()
    } finally {
      if (bufferedSource != null) {
        bufferedSource.close()
      }
    }
    query
  }


  /**
   * Retrieve sql if needed or return sql
   */

  private[this] def processQueryConfigString(query: String, configPath: Path): String = {
    if (query.endsWith(".sql")) {
      readQueryFile(configPath.resolveSibling(query).toString)
    } else {
      query
    }
  }

  /**
    * Read a table object and set the select and parallel_load query.
    * @param path String with the path to the sql-file
    * @param table the table object in which the queries should be set.
    */
  def setQueryStrings(table: Table, configPath: Path) = {
    val sources = table.getSources
    sources.foreach(s => {
      val partQuery = s.getParallel_load.getQuery
      s.getParallel_load.setQuery(processQueryConfigString(partQuery, configPath))
      val selQuery = s.getSelect.getQuery
      s.getSelect.setQuery(processQueryConfigString(selQuery, configPath))
    })

  }

}
