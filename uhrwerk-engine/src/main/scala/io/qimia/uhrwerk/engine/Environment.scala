package io.qimia.uhrwerk.engine

import io.qimia.uhrwerk.common.framemanager.FrameManager
import io.qimia.uhrwerk.common.metastore.config.TableResult
import io.qimia.uhrwerk.common.model._
import io.qimia.uhrwerk.config.YamlConfigReader
import io.qimia.uhrwerk.engine.Environment._
import org.apache.log4j.Logger

import scala.collection.mutable

object Environment {

  sealed abstract class Ident

  case class TableIdent(area: String, vertical: String, name: String, version: String) extends Ident

  case class SourceIdent(connection: String, path: String, format: String) extends Ident

  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Dynamically retrieve the Table Transformation function based on config parameters (or convention)
   *
   * @param table table which still needs the user-function (and has not been loaded yet)
    * @return user's code with the table's transformation function
    */
  def getTableFunctionDynamic(table: Table): TaskInput => TaskOutput = {
    // Dynamically load the right class and return the function described in it
    // either it is defined in the table object or through the convention of classnaming
    // `area.vertical.name.version`
    val tableTransform = Class
      .forName(table.getClassName)
      .getConstructor()
      .newInstance()
      .asInstanceOf[TableTransformation]
    tableTransform.process
  }

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
    logger.error("Storing table failed:")
    val tableStoreMsg = badTableResult.getMessage
    if ((tableStoreMsg != null) && (tableStoreMsg.length > 0)) {
      logger.error(tableStoreMsg)
    }
    if (badTableResult.isError) {
      badTableResult.getException.printStackTrace()
    }
    val targetResult = badTableResult.getTargetResult
    if (targetResult != null) {
      if (!targetResult.isSuccess) {
        logger.error(targetResult.getMessage)
      }
      if (targetResult.isError) {
        targetResult.getException.printStackTrace()
      }
    }
    val dependencyResult = badTableResult.getDependencyResult
    if (dependencyResult != null) {
      if (!dependencyResult.isSuccess) {
        logger.error(dependencyResult.getMessage)
      }
      if (dependencyResult.isError) {
        dependencyResult.getException.printStackTrace()
      }
    }
    val sourceResults = badTableResult.getSourceResults
    if ((sourceResults != null) && sourceResults.nonEmpty) {
      sourceResults.foreach(sr => {
        if (!sr.isSuccess) {
          logger.error(sr.getMessage)
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
  val metaStore: MetaStore = store

  /**
    * Add and load connections to uhrwerk
    * @param connConfigLoc location of connection configuration file
    * @param overwrite remove old definitions of table or keep them and stop if changes have been made
    */
  def addConnectionFile(connConfigLoc: String, overwrite: Boolean = false): Unit = {
    val connections = configReader.readConnections(connConfigLoc)
    addConnections(connections, overwrite)
  }

  /**
    * Add and load connections to uhrwerk
    * @param connConfigs connection configuration objects
    * @param overwrite remove old definitions of table or keep them and stop if changes have been made
    */
  def addConnections(connConfigs: Seq[Connection], overwrite: Boolean = false): Unit =
    connConfigs.foreach(conn => store.connectionService.save(conn, overwrite))

  /**
    * Add a new table to the uhrwerk dag
    * @param tableConfigLoc location of the table configuration file
    * @param userFunc user function for transforming & joining the sources and dependencies
    * @param overwrite remove old definitions of table or keep them and stop if changes have been made
    * @return If storing the config was a success a TableWrapper object
    */
  def addTableFile(tableConfigLoc: String,
                   userFunc: TaskInput => TaskOutput,
                   overwrite: Boolean = false): Option[TableWrapper] = {
    val tableYaml = configReader.readTable(tableConfigLoc)
    addTable(tableYaml, userFunc, overwrite)
  }

  /**
    * Add a new table to the uhrwerk dag environment by loading the usercode dynamically
    * @param tableConfigLoc location of table configuration file
    * @param overwrite remove old definitions of table or keep them and stop if changes have been made
    * @return
    */
  def addTableFileConvention(tableConfigLoc: String, overwrite: Boolean): Option[TableWrapper] = {
    val tableYaml = configReader.readTable(tableConfigLoc)
    addTable(tableYaml, getTableFunctionDynamic(tableYaml), overwrite)
  }

  /**
    * Add a new table to the uhrwerk dag environment by loading the usercode dynamically
    * @param tableConfig table configuration
    * @param overwrite remove old definitions of table or keep them and stop if changes have been made
    */
  def addTableConvention(tableConfig: Table, overwrite: Boolean): Unit =
    addTable(tableConfig, getTableFunctionDynamic(tableConfig), overwrite)

  /**
    * Add a new table to the uhrwerk dag
    * @param tableConfig table configuration object
    * @param userFunc user function for transforming & joining the sources and dependencies
    * @param overwrite remove old definitions of table or keep them and stop if changes have been made
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
   * Setup a full dag based on a configuration file and userCode class config or convention
   * @param dagConfigLoc location of the full dag configuration file
   */
  def setupDagFileConvention(dagConfigLoc: String, overwrite: Boolean = false): Unit = {
    val dagYaml = configReader.readDag(dagConfigLoc)
    setupDagConvention(dagYaml, overwrite)
  }

  /**
   * Setup a full dag based on a configuration
   * @param dagConfig full dag configuration object
   */
  def setupDagConvention(dagConfig: Dag, overwrite: Boolean = false){
    dagConfig.getConnections.foreach(conn => store.connectionService.save(conn, overwrite))
    dagConfig.getTables.foreach(t => {
      val ident = TableIdent(t.getArea, t.getVertical, t.getName, t.getVersion)
      val storeRes = store.tableService.save(t, overwrite)
      if (!storeRes.isSuccess) {
        logger.error(storeRes.getMessage)
      } else {
        val storedT  = storeRes.getNewResult
        val userFunc = getTableFunctionDynamic(t)
        val wrapper  = new TableWrapper(store, storedT, userFunc, frameManager)
        tables(ident) = wrapper
      }
    })
  }

  /**
   * Setup a full dag based on a configuration file
   * @param dagConfigLoc location of the full dag configuration file
   * @param userFuncs a map with the table identity objects mapped to the userfunctions for transformation
   */
  def setupDagFile(dagConfigLoc: String, userFuncs: Map[Ident, TaskInput => TaskOutput], overwrite: Boolean = false): Unit = {
    // TODO: First fix addConnections + addTable then work those changes back into this function
    val dagYaml = configReader.readDag(dagConfigLoc)
    setupDag(dagYaml, userFuncs, overwrite)
  }

  /**
    * Setup a full dag based on a configuration
    * @param dagConfig full dag configuration object
    * @param userFuncs a map with the table identity objects mapped to the userfunctions for transformation
    */
  def setupDag(dagConfig: Dag, userFuncs: Map[Ident, TaskInput => TaskOutput], overwrite: Boolean = false): Unit = {
    dagConfig.getConnections.foreach(conn => store.connectionService.save(conn, overwrite))
    dagConfig.getTables.foreach(t => {
      val ident = TableIdent(t.getArea, t.getVertical, t.getName, t.getVersion)
      if (userFuncs.contains(ident)) {
        val storeRes = store.tableService.save(t, overwrite)
        if (!storeRes.isSuccess) {
          logger.error("TableStore failed: " + storeRes.getMessage)
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
