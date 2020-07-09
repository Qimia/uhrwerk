package io.qimia.uhrwerk

import java.time.LocalDateTime
import java.util.concurrent.Executors

import io.qimia.uhrwerk.ManagedIO.FrameManager
import io.qimia.uhrwerk.StepWrapper.TaskInput
import io.qimia.uhrwerk.models.config.{Connection, Dependency}
import io.qimia.uhrwerk.utils.TimeTools
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.concurrent.duration
import scala.concurrent.{Await, ExecutionContext, Future}

object StepWrapper {

  case class TaskInput(connections: Map[String, Connection],
                       startTS: LocalDateTime,
                       frameIn: List[DataFrame])

  def getVirtualDependency() = {} // If we know it's there we can create the dataframe with a function

}

class StepWrapper(store: MetaStore, frameManager: FrameManager) {

  // Smallest execution step in our DAG (gets most parameters from configuration given by store)
  def runTasks(stepFunction: TaskInput => Option[DataFrame],
               startTimes: Seq[LocalDateTime])(implicit ex: ExecutionContext): List[Future[LocalDateTime]] = {

    def runSingleTask(time: LocalDateTime): Unit = {
      // Use metastore to denote start of the run
      val startLog = store.writeStartTask()

      // Use configured frameManager to read dataframes
      // TODO: Read the proper DF here and get the right connection/time params for it
      val inputDFs: List[DataFrame] = store.stepConfig.getDependencies
        .filter(dep => {
          !dep.isExternal
          // TODO: Filter only the ones internal and which can be read using FrameLoader (is df from managed datalake)
        })
        .map(dep => frameManager.loadDFFromLake(store.connections(dep.getConnectionName), dep, Option(time)))
        .toList
      val taskInput =
        new TaskInput(store.connections, time, inputDFs)

      // map dataframe using usercode
      val success = try {
        val frame = stepFunction(taskInput)
        // write output dataframe according to startTime and configuration
        if (frame.isDefined) {
          store.stepConfig.getTargets
            .filter(tar => {
              !tar.isExternal
              // TODO: Only when it should be written to a file
            })
            .foreach(tar =>
              frameManager.writeDFToLake(frame.get, store.connections(tar.getConnectionName), tar, Option(time)))
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

    val tasks: mutable.ListBuffer[Future[LocalDateTime]] = new mutable.ListBuffer

    checkResult
      .foreach({
        case Right(time) => {
          val runningTask = Future {
            runSingleTask(time)
            time
          }
          tasks.append(runningTask)
        }
        case Left(err) =>
          System.err.println(
            s"Couldn't run ${TimeTools.convertTSToString(err._1)}" +
              s"because of dependencies ${err._2.mkString(",")}")
      })
    tasks.toList
  }

  // If you want to execute this step for a given list of startTimes without thinking about the execution context
  // or the futures
  def runTasksAndWait(stepFunction: TaskInput => Option[DataFrame],
                      startTimes: Seq[LocalDateTime], threads: Option[Int] = Option.empty): Unit = {

    implicit val executionContext = if (threads.isEmpty) {
      ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
    } else {
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threads.get))
    }
    val futures = runTasks(stepFunction, startTimes)
    Await.result(Future.sequence(futures), duration.Duration(24, duration.HOURS))
  }

  def checkVirtualStep(
      dependency: Dependency,
      startTime: LocalDateTime): List[Either[String, LocalDateTime]] = {
    // Use metaStore to check if the right batches are there
    Nil
  }

}
