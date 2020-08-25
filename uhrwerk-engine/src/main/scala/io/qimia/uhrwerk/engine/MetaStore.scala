package io.qimia.uhrwerk.engine

import java.sql.{Driver, DriverManager}

import io.qimia.uhrwerk.common.metastore.config.{
  ConnectionService,
  PartitionDependencyService,
  PartitionService,
  TableService
}
import io.qimia.uhrwerk.common.metastore.dependency.TableDependencyService
import io.qimia.uhrwerk.common.model.{Metastore => MetastoreConnInfo}
import io.qimia.uhrwerk.dao.{
  ConnectionDAO,
  PartitionDAO,
  PartitionDependencyDAO,
  TableDAO
}

object MetaStore {

  /**
   * Build a MetaStore object based on a metastore model object which contains the connection details
   * @param connectionInfo MetaStore model object with connection info
   * @return engine metastore
   */
  def build(connectionInfo: MetastoreConnInfo): MetaStore = {
    Class.forName(connectionInfo.getJdbc_driver)
    val dbConn = DriverManager.getConnection(connectionInfo.getJdbc_url,
                                             connectionInfo.getUser,
                                             connectionInfo.getPass)
    val tableDao = new TableDAO(dbConn)
    MetaStore(new ConnectionDAO(dbConn),
              tableDao,
              tableDao,
              new PartitionDAO(dbConn),
              new PartitionDependencyDAO(dbConn))
  }
}

case class MetaStore(
    connectionService: ConnectionService,
    tableService: TableService,
    tableDependencyService: TableDependencyService,
    partitionService: PartitionService,
    partitionDependencyService: PartitionDependencyService
)
