package io.qimia.uhrwerk.engine

import io.qimia.uhrwerk.common.framemanager.FrameManager
import io.qimia.uhrwerk.common.metastore.config.TableResult
import io.qimia.uhrwerk.config.YamlConfigReader
import io.qimia.uhrwerk.engine.Environment.{Ident, TableIdent, reportProblems, tableCleaner}
import io.qimia.uhrwerk.common.model.{Connection, Dag, Dependency, PartitionTransformType, Source, Table}

import scala.collection.mutable

object Environment {
  sealed abstract class Ident
  case class TableIdent(area: String, vertical: String, name: String, version: String) extends Ident
  case class SourceIdent(connection: String, path: String, format: String)             extends Ident

  /**
    * Setup the uhrwerk environment based on an environment configuration file
    * @param envConfigLoc location of the environment config file
    * @param frameManager user made framemanager
    * @return uhrwerk environment
    */
  def build(envConfigLoc: String, frameManager: FrameManager): Environment = {
    val configReader = new YamlConfigReader()
    val metaInfo     = configReader.readEnv(envConfigLoc)
    new Environment(MetaStore.build(metaInfo), frameManager: FrameManager)
  }

  /**
    * Utility cleaner class that makes sure the sources/dependencies are initialized
    * @param table table to clean
    * @return cleaned table
    */
  def tableCleaner(table: Table): Table = {
    if (table.getDependencies == null) {
      table.setDependencies(new Array[Dependency](0))
    }
    if (table.getSources == null) {
      table.setSources(new Array[Source](0))
    }
    table
  }

  /**
    * Report table save problems to user over std.err
    * @param badTableResult Unsuccessful table store result
    */
  def reportProblems(badTableResult: TableResult): Unit = {
    System.err.println("Storing table failed:")
    val tableStoreMsg = badTableResult.getMessage
    if ((tableStoreMsg != null) && (tableStoreMsg.length > 0)) {
      System.err.println(tableStoreMsg)
    }
    if (badTableResult.isError) {
      badTableResult.getException.printStackTrace()
    }
    val targetResult = badTableResult.getTargetResult
    if (targetResult != null) {
      if (!targetResult.isSuccess) {
        System.err.println(targetResult.getMessage)
      }
      if (targetResult.isError) {
        targetResult.getException.printStackTrace()
      }
    }
    val dependencyResult = badTableResult.getDependencyResult
    if (dependencyResult != null) {
      if (!dependencyResult.isSuccess) {
        System.err.println(dependencyResult.getMessage)
      }
      if (dependencyResult.isError) {
        dependencyResult.getException.printStackTrace()
      }
    }
    val sourceResults = badTableResult.getSourceResults
    if ((sourceResults != null) && sourceResults.nonEmpty) {
      sourceResults.foreach(sr => {
        if (!sr.isSuccess) {
          System.err.println(sr.getMessage)
        }
        if (sr.isError) {
          sr.getException.printStackTrace()
        }
      })
    }
  }
}

class Environment(store: MetaStore, frameManager: FrameManager) {
  val configReader                             = new YamlConfigReader()
  val tables: mutable.Map[Ident, TableWrapper] = mutable.HashMap()

  /**
    * Add and load connections to uhrwerk
    * @param connConfigLoc location of connection configuration file
    */
  def addConnectionFile(connConfigLoc: String, overwrite: Boolean = false): Unit = {
    val connections = configReader.readConnections(connConfigLoc)
    addConnections(connections, overwrite)
  }

  /**
    * Add and load connections to uhrwerk
    * @param connConfigs connection configuration objects
    */
  def addConnections(connConfigs: Seq[Connection], overwrite: Boolean = false): Unit =
    connConfigs.foreach(conn => store.connectionService.save(conn, overwrite))

  /**
    * Add a new table to the uhrwerk dag
    * @param tableConfigLoc location of the table configuration file
    * @param userFunc user function for transforming & joining the sources and dependencies
    * @return If storing the config was a success a TableWrapper object
    */
  def addTableFile(tableConfigLoc: String,
                   userFunc: TaskInput => TaskOutput,
                   overwrite: Boolean = false): Option[TableWrapper] = {
    val tableYaml = configReader.readTable(tableConfigLoc)
    addTable(tableYaml, userFunc, overwrite)
  }

  /**
    * Add a new table to the uhrwerk dag
    * @param tableConfig table configuration object
    * @param userFunc user function for transforming & joining the sources and dependencies
    * @return If storing the config was a success a TableWrapper object
    */
  def addTable(tableConfig: Table,
               userFunc: TaskInput => TaskOutput,
               overwrite: Boolean = false): Option[TableWrapper] = {
    val cleanedTable = Environment.tableCleaner(tableConfig)
    val storeRes     = store.tableService.save(cleanedTable, overwrite)
    if (!storeRes.isSuccess) {
      // TODO: Expand the handling of store failures
      reportProblems(storeRes)
      return Option.empty
    }
    val storedTable = storeRes.getNewResult
    val ident       = TableIdent(storedTable.getArea, storedTable.getVertical, storedTable.getName, storedTable.getVersion)
    val wrapper     = new TableWrapper(store, storedTable, userFunc, frameManager)
    tables(ident) = wrapper
    Option(wrapper)
  }

  /**
    * Retrieve previously loaded TableWrapper object
    * @param id a unique table identifier case class
    * @return option with tablewrapper if found
    */
  def getTable(id: Ident): Option[TableWrapper] = tables.get(id)

  /**
    * Setup a full dag based on a configuration
    * @param dagConfigLoc location of the full dag configuration file
    * @param userFuncs a map with the table identity objects mapped to the userfunctions for transformation
    */
  def setupDagFile(dagConfigLoc: String, userFuncs: Map[Ident, TaskInput => TaskOutput]): Unit = {
    // TODO: First fix addConnections + addTable then work those changes back into this function
    val dagYaml = configReader.readDag(dagConfigLoc)
    setupDag(dagYaml, userFuncs)
  }

  /**
    * Setup a full dag based on a configuration
    * @param dagConfig full dag configuration object
    * @param userFuncs a map with the table identity objects mapped to the userfunctions for transformation
    */
  def setupDag(dagConfig: Dag, userFuncs: Map[Ident, TaskInput => TaskOutput]): Unit = {
    dagConfig.getConnections.foreach(conn => store.connectionService.save(conn, true))
    dagConfig.getTables.foreach(t => {
      val ident = TableIdent(t.getArea, t.getVertical, t.getName, t.getVersion)
      if (userFuncs.contains(ident)) {
        val storeRes = store.tableService.save(t, true)
        if (!storeRes.isSuccess) {
          System.err.println(storeRes.getMessage)
        } else {
          val storedT  = storeRes.getNewResult
          val userFunc = userFuncs(ident)
          val wrapper  = new TableWrapper(store, storedT, userFunc, frameManager)
          tables(ident) = wrapper
        }
      }
    })
  }

}
