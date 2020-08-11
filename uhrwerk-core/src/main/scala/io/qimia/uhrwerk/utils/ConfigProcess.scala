package io.qimia.uhrwerk.utils

import java.time.temporal.ChronoUnit

import io.qimia.uhrwerk.config.{ConnectionType, DependencyType}
import io.qimia.uhrwerk.config.model.{Global, Table}

object ConfigProcess {
  val UHRWERK_BACKEND_SQL_CREATE_TABLES_FILE = "metastore_ddl_mysql.sql" // todo this might be configurable in the future
  val UHRWERK_BACKEND_CONNECTION_NAME
    : String = "uhrwerk-backend" // todo this might be configurable in the future

  /**
    * Overall preparation of a new config. Will add missing fields based on filled in fields and
    * will check if the given configuration is valid or not (based on what is only in the config)
    * (This does **not** include warnings based on previously persisted data)
    *
    * @param step   A single step configuration
    * @param global A global configuration (connection-information)
    * @return Did the config validate correctly or not
    */
  def enrichAndValidateConfig(step: Table, global: Global): Boolean = {
    if (!checkFieldsConfig(step, global)) {
      return false
    }
    autofillPartitionSizes(step)
    if (!checkAndUpdateAgg(step)) {
      return false
    }
    if (!checkInTableTimes(step)) {
      return false
    }
    true
  }

  /**
    * Check if path-names and connection-names have been filled in.
    * Also checks whether there is a connection for the backend.
    *
    * Auto-fills jdbc format if connection is jdbc
    *
    * @param table
    * @param global
    * @return
    */
  def checkFieldsConfig(table: Table, global: Global): Boolean = {
    val connectionTypes =
      global.getConnections.map(conn => conn.getName -> conn.getTypeEnum).toMap
    if (table.getPartitionSize == "") {
      System.err.println("Missing partition size for table")
      return false
    }
    // TODO: Require a step name or not?

    if (!connectionTypes.contains(UHRWERK_BACKEND_CONNECTION_NAME)) {
      return false
    }

    if (table.dependenciesSet()) {
      val dependencies = table.getDependencies
      dependencies.foreach(d => {
        if (!connectionTypes.contains(d.getConnectionName)) {
          System.err.println("Unknown connection name for dependency")
          return false
        }
        if (d.getFormat == "") {
          val connType = connectionTypes(d.getConnectionName)
          if (connType == ConnectionType.JDBC) {
            d.setFormat("jdbc")
          } else {
            System.err.println("Missing format for dependency")
            return false
          }
        }
      })
    }
    if (table.sourcesSet()) {
      val sources = table.getSources
      sources.foreach(s => {
        if (!connectionTypes.contains(s.getConnectionName)) {
          System.err.println("Unknown connection name for source")
          return false
        }
        if (s.getFormat == "") {
          val connType = connectionTypes(s.getConnectionName)
          if (connType == ConnectionType.JDBC) {
            s.setFormat("jdbc")
          } else {
            System.err.println("Missing format for source")
            return false
          }
        }
        if (s.getPath == "") {
          System.err.println("Missing path for source")
          return false
        }
      })
    }
    val targets = table.getTargets
    targets.foreach(t => {
      if (!connectionTypes.contains(t.getConnectionName)) {
        System.err.println("Unknown connection name for target")
        return false
      }
      if (t.getFormat == "") {
        val connType = connectionTypes(t.getConnectionName)
        if (connType == ConnectionType.JDBC) {
          t.setFormat("jdbc")
        } else {
          System.err.println("Missing format for target")
          return false
        }
      }
    })
    true
  }

  /**
    * Specifically check (and fill in) partition sizes of aggregate dependencies
    */
  def checkAndUpdateAgg(in: Table): Boolean = {
    val targetPartitionSize = in.getPartitionSizeDuration
    if (in.dependenciesSet()) {
      val aggDependencies = in.getDependencies.filter(d =>
        d.getTypeEnum == DependencyType.AGGREGATE)
      aggDependencies.foreach(ad => {
        val aggSize = ad.getPartitionSize
        val aggCount: Int = ad.getPartitionCount
        val sizeSet = aggSize != ""
        val countSet = aggCount != 1
        if (!sizeSet && !countSet) {
          System.err.println(
            "Need to set either size or a count (other than 1) for an aggregate dependency")
          return false
        } else if (sizeSet && !countSet) {
          val aggSized = ad.getPartitionSizeDuration
          if (aggSized == targetPartitionSize) {
            System.err.println(
              "Use one on one for same-partition-sized dependencies (and not aggregate)")
            return false
          } else if (TimeTools.divisibleBy(targetPartitionSize, aggSized)) {
            ad.setPartitionCount(
              (targetPartitionSize.toMinutes / aggSized.toMinutes).toInt)
          } else {
            System.err.println(
              "Can't divide the partition size of the target table " +
                "by the partition size of the aggregate dependency")
            return false
          }
        } else if (!sizeSet && countSet) {
          // TODO: This method needs proper testing
          val aggCountSize = targetPartitionSize
            .dividedBy(aggCount)
            .truncatedTo(ChronoUnit.MINUTES)
          if (TimeTools.divisibleBy(targetPartitionSize, aggCountSize)) {
            ad.setPartitionSize(TimeTools.convertDurationToStr(aggCountSize))
          } else {
            System.err.println(
              s"The target batch size can not be nicely split into ${aggCount} batches")
            return false
          }
        } else {
          val aggSized = ad.getPartitionSizeDuration
          val aggCountSize = targetPartitionSize.dividedBy(aggCount)
          if (aggSized != aggCountSize) {
            System.err.println("Aggregate batch size and count does not agree")
            return false
          }
        }
      })
    }
    true
  }

  // If no partition-sizes have been given then take the one set for the whole step
  /**
    * Take the step's batch size and use it as a partition size of any inTable (except aggregates) or target
    *
    * @param in
    */
  def autofillPartitionSizes(in: Table): Unit = {
    val batchSize = in.getPartitionSize // Must be set now

    if (in.sourcesSet()) {
      val sources = in.getSources
      sources.foreach(s => {
        val partSize = s.getPartitionSize
        if (partSize == "") {
          s.setPartitionSize(batchSize)
        }
      })
    }
    if (in.dependenciesSet()) {
      val dependencies = in.getDependencies
      dependencies.foreach(d => {
        val partSize = d.getPartitionSize
        if (d.getTypeEnum != DependencyType.AGGREGATE) {
          if (partSize == "") {
            d.setPartitionSize(batchSize)
          }
        }
      })
    }
  }

  /**
    * Check if all input partition sizes are in accordance to the target partition size meaning:
    * - They are not bigger than the target partition size
    * - They are equal size for oneonone & window dependencies
    * - They are equal size for sources
    * Warning: Does not check aggregate dependencies (should be done separately)
    */
  def checkInTableTimes(in: Table): Boolean = {
    val targetPartitionSize = in.getPartitionSizeDuration
    if (in.sourcesSet()) {
      val sources = in.getSources
      sources.foreach(s => {
        val partSize = s.getPartitionSizeDuration
        if (partSize != targetPartitionSize) {
          System.err.println("Source has different partition size as the table")
          return false
        }
      })
    }
    if (in.dependenciesSet()) {
      val dependencies = in.getDependencies
      dependencies.foreach(d => {
        d.getTypeEnum match {
          case DependencyType.ONEONONE => {
            if (d.getPartitionSizeDuration != targetPartitionSize) {
              System.err.println(
                "Dependency has bad partition size wrt. target partition size")
              return false
            }
          }
          case DependencyType.WINDOW => {
            if (d.getPartitionSizeDuration != targetPartitionSize) {
              System.err.println(
                "Dependency has bad partition size wrt. target partition size")
              return false
            }
          }
          case _ =>
        }
      })
    }
    true
  }

}
