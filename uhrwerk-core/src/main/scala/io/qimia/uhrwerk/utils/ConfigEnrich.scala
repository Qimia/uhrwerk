package io.qimia.uhrwerk.utils

import io.qimia.uhrwerk.models.config.{Step, Table}

object ConfigEnrich {

  // If no partition-sizes have been given then take the global one (WARNING MUTATES)
  def autofillStepPartitionSizes(in: Step): Unit = {
    val batchSize = in.getBatchSize
    if (batchSize == "") {
      return
    }

    val sourceTables = if (in.sourcesSet()) {
      in.getSources.toList
    } else {
      Nil
    }
    val dependencyTables = if (in.dependenciesSet()) {
      in.getDependencies.toList
    } else {
      Nil
    }
    val allTables: List[Table] = sourceTables ::: dependencyTables ::: in.getTargets.toList
    allTables.foreach(t => {
      val partSize = t.getPartitionSize
      if (partSize == "") {
        t.setPartitionSize(batchSize)
      }
    })
  }

}
