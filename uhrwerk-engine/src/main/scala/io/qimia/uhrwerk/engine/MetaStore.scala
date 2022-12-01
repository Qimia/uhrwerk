package io.qimia.uhrwerk.engine

import io.qimia.uhrwerk.common.metastore.config._
import io.qimia.uhrwerk.common.metastore.dependency.TableDependencyService
import io.qimia.uhrwerk.common.metastore.model.{MetastoreModel => MetastoreConnInfo}
import io.qimia.uhrwerk.dao._
import io.qimia.uhrwerk.repo.HikariCPDataSource

import java.sql.DriverManager

object MetaStore {

  /** Build a MetaStore object based on a metastore model object which contains the connection details
    * @param connectionInfo MetaStore model object with connection info
    * @return engine metastore
    */
  def build(connectionInfo: MetastoreConnInfo): MetaStore = {
    Class.forName(connectionInfo.getJdbc_driver)

    HikariCPDataSource.initConfig(
      connectionInfo.getJdbc_url,
      connectionInfo.getUser,
      connectionInfo.getPass
    )

    val tableDao = new TableDAO()
    MetaStore(
      new ConnectionDAO(),
      tableDao,
      new TargetDAO(),
      tableDao,
      new PartitionDAO(),
      new PartitionDependencyDAO()
    )
  }
}

case class MetaStore(
    connectionService: ConnectionService,
    tableService: TableService,
    targetService: TargetService,
    tableDependencyService: TableDependencyService,
    partitionService: PartitionService,
    partitionDependencyService: PartitionDependencyService
)
