package io.qimia.uhrwerk

import java.time.LocalDateTime

import io.qimia.uhrwerk.models.config.Dependency
import org.apache.spark.sql.DataFrame

object StepWrapper {
  def getVirtualDependency() = {}   // If we know it's there we can create the dataframe with a function

}

class StepWrapper(store: MetaStore) {

  // Smallest execution step in our DAG (gets most parameters from configuration given by store)
  def runFunction(stepFunction: DataFrame => DataFrame,
                  startTime: LocalDateTime,
                  sourceCodeId: Int): Unit = {

    // Use metastore to check the dependencies from the metastore

    /// Check Real ones first
    /// If not found -> stop

    /// Check virtual

    // Use metastore to denote start of the run

    // Use configured frameManager to read dataframes

    // map dataframe using usercode

    // write output dataframe according to startTime and configuration

    // Use metastore to denote end of the run

  }


  def checkVirtualStep(dependency: Dependency, startTime: LocalDateTime): List[Either[String, LocalDateTime]] = {
    // Use metaStore to check if the right batches are there

    Nil
  }

}
