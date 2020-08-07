package io.qimia.uhrwerk

import java.time.{Duration, LocalDateTime}
import java.util.concurrent.Executors

import io.qimia.uhrwerk.ManagedIO.FrameManager
import io.qimia.uhrwerk.MetaStore.{DependencyFailed, DependencySuccess}
import io.qimia.uhrwerk.StepWrapper.TaskInput
import io.qimia.uhrwerk.models.{ConnectionType, DependencyType}
import io.qimia.uhrwerk.models.config.{Connection, Dependency}
import io.qimia.uhrwerk.utils.TimeTools
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration
import scala.concurrent.{Await, ExecutionContext, Future}

object StepWrapper {

  case class TaskInput(connections: Map[String, Connection],
                       startTS: LocalDateTime,
                       frameIn: List[DataFrame])

  def getVirtualDependency() = {} // If we know it's there we can create the dataframe with a function

  // combine the 2 dependency check outputs of different checkers (different virtual ones or metastore)
  // Assumes the lists are equal size and ordered the same
  def combineDependencyChecks(
      a: List[Either[DependencyFailed, DependencySuccess]],
      b: List[Either[DependencyFailed, DependencySuccess]])
    : List[Either[DependencyFailed, DependencySuccess]] = {

    def combineEither[R](
        a: Either[DependencyFailed, R],
        b: Either[DependencyFailed, R]): Either[DependencyFailed, R] = {
      (a, b) match {
        case (Left(a1), Left(b1)) => {
          Left(new DependencyFailed(a1._1, a1._2 union b1._2))
        }
        case (Left(a2), Right(_))  => Left(a2)
        case (Right(_), Left(b3))  => Left(b3)
        case (Right(a4), Right(_)) => Right(a4)
      }
    }

    assert(a.length == b.length)
    a.zip(b).map(tup => combineEither(tup._1, tup._2))
  }

  // Create a boolean filter for the window batches based on window size
  def createFilterWithWindowList(windowBatchList: List[Boolean],
                                 windowSize: Int): List[Boolean] = {
    var toRemove = 0 // TODO proper map without side effects
    windowBatchList.map {
      case false => {
        toRemove = windowSize - 1
        false
      }
      case true => {
        if (toRemove > 0) {
          toRemove -= 1
          false
        } else {
          true
        }
      }
    }
  }

  // apply a boolean filter to an existing dependency-check output
  def applyWindowFilter(
      originalOutcome: List[Either[DependencyFailed, DependencySuccess]],
      filter: List[Boolean],
      nameDependency: String)
    : List[Either[DependencyFailed, DependencySuccess]] = {
    assert(originalOutcome.length == filter.length)
    originalOutcome
      .zip(filter)
      .map(tup => {
        if (tup._2) {
          tup._1
        } else {
          tup._1 match {
            case Right(time) =>
              Left(new DependencyFailed(time, Set(nameDependency)))
            case Left(f) => Left(f.copy(_2 = f._2 + nameDependency))
          }
        }
      })
  }
}

class StepWrapper(store: MetaStore, frameManager: FrameManager) {

  // Smallest execution step in our DAG (gets most parameters from configuration given by store)
  def runTasks(stepFunction: TaskInput => Option[DataFrame],
               startTimes: Seq[LocalDateTime])(
      implicit ex: ExecutionContext): List[Future[LocalDateTime]] = {

    // Run a step for a single partition (denoted by starting-time)
    def runSingleTask(time: LocalDateTime): Unit = {
      val startLog = store.logStartTask()

      // Use configured frameManager to read dataframes
      // TODO: Read the proper DF here and get the right connection/time params for it
      // Note that *we are responsible* for all (standard) loading of DataFrames!
      val inputDepDFs: List[DataFrame] = if (store.stepConfig.dependenciesSet) {
        store.stepConfig.getDependencies
          .map(
            dep =>
              frameManager.loadDataFrame(
                store.connections(dep.getConnectionName),
                dep,
                Option(time)))
          .toList
      } else {
        Nil
      }
      // TODO: Implement the Source data loading in a similar fashion to the dependency loading
      val inputSourceDFs: List[DataFrame] = Nil
      val taskInput =
        TaskInput(store.connections, time, inputDepDFs ::: inputSourceDFs)

      val success = try {
        val frame = stepFunction(taskInput)
        // TODO error checking: if target should be on datalake but no frame is given
        // Note: We are responsible for all standard writing of DataFrames
        if (frame.isDefined) {
          store.stepConfig.getTargets
            .foreach(
              tar =>
                frameManager.writeDataFrame(
                  frame.get,
                  store.connections(tar.getConnectionName),
                  tar,
                  Option(time)))
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

      store.logFinishTask(startLog, time, success)
    }

    val checkResult: List[Either[DependencyFailed, DependencySuccess]] =
      if (store.stepConfig.dependenciesSet) {
        val dependenciesByVirtual =
          store.stepConfig.getDependencies.groupBy(dep =>
            dep.getTypeEnum != DependencyType.ONEONONE)
        dependenciesByVirtual
          .map(keyDeps =>
            if (keyDeps._1) {
              val individualVirtOutcomes =
                keyDeps._2.map(checkVirtualStep(_, startTimes.toList))
              individualVirtOutcomes.reduce(StepWrapper.combineDependencyChecks)
            } else {
              store.checkDependencies(keyDeps._2, startTimes)
          })
          .reduce(StepWrapper.combineDependencyChecks)
      } else {
        startTimes.map(Right(_)).toList
      }

    // TODO need to add filtering and logic for Virtual Steps and other kinds of steps

    val tasks: mutable.ListBuffer[Future[LocalDateTime]] =
      new mutable.ListBuffer

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
                      startTimes: Seq[LocalDateTime],
                      threads: Option[Int] = Option.empty): Unit = {

    implicit val executionContext = if (threads.isEmpty) {
      ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
    } else {
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threads.get))
    }
    val futures = runTasks(stepFunction, startTimes)
    Await.result(Future.sequence(futures),
                 duration.Duration(24, duration.HOURS))
  }

  def checkVirtualStep(dependency: Dependency, startTimes: List[LocalDateTime])
    : List[Either[DependencyFailed, DependencySuccess]] = {
    // Use metaStore to check if the right batches are there

    def checkWindowStep(): List[Either[DependencyFailed, DependencySuccess]] = {
      val windowSize = dependency.getPartitionCount
      val windowStartTimes = TimeTools.convertToWindowBatchList(
        startTimes,
        store.stepConfig.getBatchSizeDuration,
        windowSize)

      val windowedBatchOutcome =
        store.checkDependencies(Array(dependency), windowStartTimes)

      val booleanOutcome = windowedBatchOutcome.map({
        case Left(_)  => false
        case Right(_) => true
      })
      val windowFilter =
        StepWrapper.createFilterWithWindowList(booleanOutcome, windowSize)
      val filteredOutcome = StepWrapper.applyWindowFilter(windowedBatchOutcome,
                                                          windowFilter,
                                                          dependency.getPath)
      // TODO: Change to name in-case we switch from using the paths to using identifiers/names

      TimeTools.cleanWindowBatchList(filteredOutcome, windowSize)
    }

    def checkAggregateStep()
      : List[Either[DependencyFailed, DependencySuccess]] = {
      // TODO: Doesn't check if the given duration (dep.getPartitionSizeDuration) agrees with this division
      val smallerStartTimes = TimeTools.convertToSmallerBatchList(
        startTimes,
        store.stepConfig.getBatchSizeDuration,
        dependency.getPartitionCount
      )
      val smallBatchOutcome =
        store.checkDependencies(Array(dependency), smallerStartTimes)
      val foundSmallBatches =
        smallBatchOutcome
          .filter(_.isRight)
          .map({
            case Right(x) => x
            case Left(_)  => throw new RuntimeException
          })
          .toSet
      val foundBigBatches = TimeTools
        .filterBySmallerBatchList(startTimes,
                                  store.stepConfig.getBatchSizeDuration,
                                  dependency.getPartitionCount,
                                  foundSmallBatches)
        .toSet

      startTimes.map(datetime =>
        if (foundBigBatches.contains(datetime)) {
          Right(datetime)
        } else {
          Left((datetime, Set(dependency.getPath)))
      })
    }

    dependency.getTypeEnum match {
      case DependencyType.AGGREGATE => checkAggregateStep()
      case DependencyType.WINDOW    => checkWindowStep()
      case _ => {
        System.err.println("Error: trying not implemented virtual step")
        throw new NotImplementedError
      }
    }
  }

}
