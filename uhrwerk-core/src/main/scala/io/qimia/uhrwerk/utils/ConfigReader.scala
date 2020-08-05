package io.qimia.uhrwerk.utils

import java.io.{FileInputStream, FileNotFoundException, IOException}
import java.nio.file.Path

import io.qimia.uhrwerk.models.config.{Global, Step}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.representer.Representer

import scala.io.{BufferedSource, Source}


object ConfigReader {

  /**
   * Read a global-configuration yaml file
   * @param path path to the yaml file
   * @return the global-config object
   */
  def readGlobalConfig(path: Path): Global = {
    val fileStream   = new FileInputStream(path.toFile)
    val representer = new Representer
    representer.getPropertyUtils.setSkipMissingProperties(true)
    val yaml   = new Yaml(new Constructor(classOf[Global]), representer)
    val config = yaml.load(fileStream).asInstanceOf[Global]
    config
  }

  /**
   * Read a single step-configuration yaml file
   * @param path path to the yaml file
   * @return the step-config object
   */
  def readStepConfig(path: Path): Step = {
    val fileStream   = new FileInputStream(path.toFile)
    val representer = new Representer
    representer.getPropertyUtils.setSkipMissingProperties(true)
    val yaml   = new Yaml(new Constructor(classOf[Step]), representer)
    val config = yaml.load(fileStream).asInstanceOf[Step]
    if (config.sourcesSet()) {
      val sources = config.getSources
      sources.foreach(s => {
        val partQuery = s.getPartitionQuery
        s.setPartitionQuery(processQueryConfigString(partQuery, path))
        val selQuery = s.getSelectQuery
        s.setSelectQuery(processQueryConfigString(selQuery, path))
      })
    }
    if (config.dependenciesSet()) {
      val dependencies = config.getDependencies
      dependencies.foreach(s => {
        val partQuery = s.getPartitionQuery
        s.setPartitionQuery(processQueryConfigString(partQuery, path))
        val selQuery = s.getSelectQuery
        s.setSelectQuery(processQueryConfigString(selQuery, path))
      })
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

}
