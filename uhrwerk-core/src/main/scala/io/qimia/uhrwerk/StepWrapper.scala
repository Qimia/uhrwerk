package io.qimia.uhrwerk

import java.time.LocalDateTime

import io.qimia.uhrwerk.ManagedIO.FrameManager
import io.qimia.uhrwerk.StepWrapper.TaskInput
import io.qimia.uhrwerk.models.config.{Connection, Dependency}
import io.qimia.uhrwerk.utils.TimeTools
import org.apache.spark.sql.DataFrame

object StepWrapper {

  case class TaskInput(connections: Array[Connection],
                       startTS: LocalDateTime,
                       frameIn: List[DataFrame])

  def getVirtualDependency() = {} // If we know it's there we can create the dataframe with a function

}

class StepWrapper(store: MetaStore, frameManager: FrameManager) {

  // Smallest execution step in our DAG (gets most parameters from configuration given by store)
  def runTasks(stepFunction: TaskInput => Option[DataFrame],
               startTimes: Seq[LocalDateTime]): Unit = {

    def runSingleTask(time: LocalDateTime): Unit = {
      // Use metastore to denote start of the run
      val startLog = store.writeStartTask()

      // Use configured frameManager to read dataframes
      // TODO: Read the proper DF here and get the right connection/time params for it
      val inputDFs: List[DataFrame] = store.stepConfig.getDependencies
        .filter(dep => {
          true // TODO: Filter only the ones internal and which can be read using FrameLoader (is df from managed datalake)
        })
        .map(dep => frameManager.loadDFFromLake(null, dep, Option(time)))
        .toList
      val taskInput =
        new TaskInput(store.globalConfig.getConnections, time, inputDFs)

      // map dataframe using usercode
      val success = try {
        val frame = stepFunction(taskInput)
        // write output dataframe according to startTime and configuration
        if (frame.isDefined) {
          store.stepConfig.getTargets
            .filter(tar => {
              true // TODO: Only when it should be written to a file
            })
            .foreach(tar =>
              frameManager.writeDFToLake(frame.get, null, tar, Option(time)))
              // TODO: add right connection info
        }
        true
      } catch {
        case e: Throwable => {
          System.err.println("Task failed: " + time.toString)
          e.printStackTrace()
          false
        }
      }

      // Use metastore to denote end of the run
      store.writeFinishTask(startLog, time, success)
      // TODO: At the moment always stores all TaskLogs
    }

    // Use metastore to check the dependencies from the metastore
    val checkResult =
      store.checkDependencies(store.stepConfig.getDependencies, startTimes)
    // TODO need to add filtering and logic for Virtual Steps and other kinds of steps

    checkResult
      .foreach({
        case Right(time) => runSingleTask(time)
        case Left(err) =>
          System.err.println(
            s"Couldn't run ${TimeTools.convertTSToString(err._1)}" +
              s"because of dependencies ${err._2.mkString(",")}")
      })
  }

  def checkVirtualStep(
      dependency: Dependency,
      startTime: LocalDateTime): List[Either[String, LocalDateTime]] = {
    // Use metaStore to check if the right batches are there
    Nil
  }

}
