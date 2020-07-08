package io.qimia.uhrwerk.utils

import io.qimia.uhrwerk.models.config._
import io.qimia.uhrwerk.models.store._
import javax.persistence.EntityManager
import collection.JavaConverters._

object ConfigPersist {

  // Need to find out which TableInfo are already in the storage and re-use
  // those if possible

  // Need to find out when to write a new row of configuration (preferably
  // we reference the old one if nothing has changed, but does this comparison lead
  // to extra requests and less performance?)

  def getOrCreateStepRef(store: EntityManager, step: Step): StepConfig = {

    val stored = store
      .createQuery(
        s"FROM StepConfig WHERE name = '${step.getName}' " +
          "AND batchSize = :batchsize " +
          s"AND parallelism = ${step.getParallelism} " +
          s"AND maxBatches = ${step.getMaxBatches} " +
          s"AND stepType = '${step.getStepType}'",
        classOf[StepConfig]
      )
      .setParameter("batchsize",
                    TimeTools.convertDurationToObj(step.getBatchSize))
      .getResultList
      .asScala
    if (stored.nonEmpty) {
      return stored.head
    }

    val stepConf = new StepConfig(
      step.getName,
      TimeTools.convertDurationToObj(step.getBatchSize),
      step.getParallelism,
      step.getMaxBatches,
      step.getStepType
    )
    store.persist(stepConf)
    stepConf
  }

  def getOrCreateConnectionRef(store: EntityManager,
                               connection: Connection): ConnectionConfig = {
    val stored = store
      .createQuery(
        s"FROM ConnectionConfig WHERE name = '${connection.getName}' " +
        s"AND type = '${connection.getType}' " +
        s"AND url = '${connection.getJdbcUri}' " +
        s"AND version = ${connection.getVersion} ",
        classOf[ConnectionConfig]
      )
      .getResultList
      .asScala
    if (stored.nonEmpty) {
      return stored.head
    }

    val connConf = new ConnectionConfig(
      connection.getName,
      connection.getType,
      connection.getJdbcUri,
      connection.getVersion
    )
    store.persist(connConf)
    connConf
  }

  def getOrCreateTableRef(store: EntityManager,
                          table: Table,
                          connectionConfig: ConnectionConfig): TableInfo = {
    val stored = store
      .createQuery(
        s"FROM TableInfo WHERE path = '${table.getPath}' " +
          s"AND area = '${table.getArea}' " +
          s"AND vertical = '${table.getVertical}' " +
          s"AND version = ${table.getVersion} " +
          s"AND connectionName = '${connectionConfig.getName}' " +
          "AND connection = :conn " +
          "AND external = :ext ",
        classOf[TableInfo]
      )
      .setParameter("conn", connectionConfig)
      .setParameter("ext", table.isExternal)
      .getResultList
      .asScala
    if (stored.nonEmpty) {
      return stored.head
    }

    val tabInfo = new TableInfo(
      table.getPath,
      table.getArea,
      table.getVertical,
      table.getVersion,
      table.getConnectionName,
      connectionConfig,
      table.isExternal
    )
    store.persist(tabInfo)
    tabInfo
  }

  def getOrCreateTargetConfig(store: EntityManager,
                              target: Target,
                              tableUsed: TableInfo,
                              step: StepConfig): TargetConfig = {
    val stored = store
      .createQuery(
        "FROM TargetConfig WHERE table = :tab " +
        "AND partitionSize = :dur " +
        "AND stepConfig = :step " +
        s"AND type = '${target.getType}' ",
        classOf[TargetConfig]
      )
      .setParameter("tab", tableUsed)
      .setParameter("dur", TimeTools.convertDurationToObj(target.getPartitionSize))
      .setParameter("step", step)
      .getResultList
      .asScala
    if (stored.nonEmpty) {
      return stored.head
    }

    val tarConf = new TargetConfig(
      tableUsed,
      TimeTools.convertDurationToObj(target.getPartitionSize),
      step,
      target.getType
    )
    store.persist(tarConf)
    tarConf
  }

  def getOrCreateDependencyConfig(store: EntityManager,
                                  dependency: Dependency,
                                  tableUsed: TableInfo,
                                  step: StepConfig): DependencyConfig = {
    val stored = store
      .createQuery(
        "FROM DependencyConfig WHERE table = :tab " +
          "AND partitionSize = :dur " +
          s"AND partitionCount = ${dependency.getPartitionCount} " +
          "AND stepConfig = :step " +
          s"AND type = '${dependency.getType}' ",
        classOf[DependencyConfig]
      )
      .setParameter("tab", tableUsed)
      .setParameter("dur", TimeTools.convertDurationToObj(dependency.getPartitionSize))
      .setParameter("step", step)
      .getResultList
      .asScala
    if (stored.nonEmpty) {
      return stored.head
    }

    val depConf = new DependencyConfig(
      tableUsed,
      TimeTools.convertDurationToObj(dependency.getPartitionSize),
      dependency.getPartitionCount,
      step,
      dependency.getType
    )
    store.persist(depConf)
    depConf
  }

  def persistStep(store: EntityManager,
                  step: Step,
                  global: Global): StepConfig = {

    val connectionSearch =
      global.getConnections.map(conn => conn.getName -> conn).toMap

    val stepStored = getOrCreateStepRef(store, step)
    step.getDependencies.foreach(dep => {
      val conn = connectionSearch(dep.getConnectionName)
      val connStored = getOrCreateConnectionRef(store, conn)
      val tableStored = getOrCreateTableRef(store, dep, connStored)
      getOrCreateDependencyConfig(store, dep, tableStored, stepStored)
    })
    step.getTargets.foreach(tar => {
      val conn = connectionSearch(tar.getConnectionName)
      val connStored = getOrCreateConnectionRef(store, conn)
      val tableStored = getOrCreateTableRef(store, tar, connStored)
      getOrCreateTargetConfig(store, tar, tableStored, stepStored)
    })

    stepStored
  }

}
