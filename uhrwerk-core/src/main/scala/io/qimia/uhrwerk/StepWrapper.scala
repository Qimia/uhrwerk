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
            case Left(f) => Left(f)
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

    def runSingleTask(time: LocalDateTime): Unit = {
      val startLog = store.logStartTask()

      // Use configured frameManager to read dataframes
      // TODO: Read the proper DF here and get the right connection/time params for it
      val inputDFs: List[DataFrame] = store.stepConfig.getDependencies
        .filter(dep => {
          !dep.isExternal && store.connections(dep.getConnectionName).getTypeEnum.isSparkPath
          // Filter only the ones internal and which can be read using FrameLoader (is df from managed datalake)
          // TODO: Discuss if this is how we decide load-by-library / load-by-user
        })
        .map(dep =>
          frameManager.loadDFFromLake(store.connections(dep.getConnectionName),
                                      dep,
                                      Option(time)))
        .toList
      val taskInput = TaskInput(store.connections, time, inputDFs)

      val success = try {
        val frame = stepFunction(taskInput)
        // TODO error checking: if target should be on datalake but no frame is given
        if (frame.isDefined) {
          store.stepConfig.getTargets
            .filter(tar => {
              !tar.isExternal
              // TODO: Only when it should be written to a file
            })
            .foreach(
              tar =>
                frameManager.writeDFToLake(
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

    val dependenciesByVirtual = store.stepConfig.getDependencies.groupBy(dep =>
      dep.getTypeEnum != DependencyType.ONEONONE)
    val checkResult = dependenciesByVirtual
      .map(keyDeps =>
        keyDeps._1 match {
          case false => store.checkDependencies(keyDeps._2, startTimes)
          case true => {
            val individualVirtOutcomes =
              keyDeps._2.map(checkVirtualStep(_, startTimes.toList))
            individualVirtOutcomes.reduce(StepWrapper.combineDependencyChecks)
          }
      })
      .reduce(StepWrapper.combineDependencyChecks)
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
        smallBatchOutcome.filter(_.isRight).map({ case Right(x) => x }).toSet
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
    }
  }

}
