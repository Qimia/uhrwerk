package io.qimia.uhrwerk

import java.time.{Duration, LocalDateTime}
import java.util.concurrent.Executors

import io.qimia.uhrwerk.ManagedIO.FrameManager
import io.qimia.uhrwerk.MetaStore.{DependencyFailedOld, DependencySuccess}
import io.qimia.uhrwerk.TableWrapper.TaskInput
import io.qimia.uhrwerk.config.{ConnectionType, DependencyType}
import io.qimia.uhrwerk.config.model.{Connection, Dependency}
import io.qimia.uhrwerk.utils.TimeTools
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration
import scala.concurrent.{Await, ExecutionContext, Future}

object TableWrapper {

  case class TaskInput(connections: Map[String, Connection],
                       startTS: LocalDateTime,
                       frameIn: List[DataFrame])

  def getVirtualDependency() = {} // If we know it's there we can create the dataframe with a function

  /**
    * Combine different dependency checking outputs. Important when combining virtual-steps with one on one dependencies
    * in a single check. Warning: Assumes that exactly the same datetime partitions have been used and returned for a and b.
    * This means the same ordering of datetimes for a and b.
    *
    * If one of the two dependencies was not met than it means it failed with those missing dependencies.
    * If both failed than the union of missing dependencies is taken
    * @param a a dependency check output
    * @param b a different dependency check output
    * @return combined result
    */
  def combineDependencyChecks(
      a: List[Either[DependencyFailedOld, DependencySuccess]],
      b: List[Either[DependencyFailedOld, DependencySuccess]])
    : List[Either[DependencyFailedOld, DependencySuccess]] = {

    def combineEither[R](
        a: Either[DependencyFailedOld, R],
        b: Either[DependencyFailedOld, R]): Either[DependencyFailedOld, R] = {
      (a, b) match {
        case (Left(a1), Left(b1)) => {
          Left(new DependencyFailedOld(a1._1, a1._2 union b1._2))
        }
        case (Left(a2), Right(_))  => Left(a2)
        case (Right(_), Left(b3))  => Left(b3)
        case (Right(a4), Right(_)) => Right(a4)
      }
    }

    assert(a.length == b.length)
    a.zip(b).map(tup => combineEither(tup._1, tup._2))
  }

  /**
    * Create a boolean window filter based on an existing list of boolean values (representing partitions found/missing)
    * and the size of the window. For example a window 3 means that the next 2 "values" should also be false.
    * @param windowBatchList List of boolean values (representing batches found or missing)
    * @param windowSize Size of the window of effect of the false values.
    * @return New boolean filter with the window applied
    */
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

  /**
    * Apply some boolean filter to a dependency check outcome
    * @param originalOutcome dependency check outcome
    * @param filter extra filter that needs to be applied
    * @param nameDependency name of the dependency that is set to be missing if filter is false
    * @return new dependency check outcome with the filter applied
    */
  def applyWindowFilter(
      originalOutcome: List[Either[DependencyFailedOld, DependencySuccess]],
      filter: List[Boolean],
      nameDependency: String)
    : List[Either[DependencyFailedOld, DependencySuccess]] = {
    assert(originalOutcome.length == filter.length)
    originalOutcome
      .zip(filter)
      .map(tup => {
        if (tup._2) {
          tup._1
        } else {
          tup._1 match {
            case Right(time) =>
              Left(new DependencyFailedOld(time, Set(nameDependency)))
            case Left(f) => Left(f.copy(_2 = f._2 + nameDependency))
          }
        }
      })
  }
}

class TableWrapper(store: MetaStore, frameManager: FrameManager) {

  /**
    * Process a single table for a list for partitionTS
    * @param TableFunction User-defined function which combines the dependencies and processed the data
    * @param startTimes sequence of partitionTS which need to be processed
    * @param ex context on which the futures run for the different batches
    * @return a list of futures containing the batch partition times
    */
  def runTasks(TableFunction: TaskInput => Option[DataFrame],
               startTimes: Seq[LocalDateTime])(
      implicit ex: ExecutionContext): List[Future[LocalDateTime]] = {

    // Run a Table for a single partition (denoted by starting-time)
    def runSingleTask(time: LocalDateTime, targets: Set[String]): Unit = {
      val startLog = store.logStartTask()

      // Use configured frameManager to read dataframes
      // TODO: Read the proper DF here and get the right connection/time params for it
      // Note that *we are responsible* for all (standard) loading of DataFrames!
      val inputDepDFs: List[DataFrame] =
        if (store.tableConfig.dependenciesSet) {
          store.tableConfig.getDependencies
            .map(
              dep =>
                frameManager.loadDependencyDataFrame(
                  store.connections(dep.getConnectionName),
                  dep,
                  Option(time)))
            .toList
        } else {
          Nil
        }
      // TODO: Implement the Source data loading in a similar fashion to the dependency loading
      val inputSourceDFs: List[DataFrame] = if (store.tableConfig.sourcesSet) {
        store.tableConfig.getSources
          .map(
            sou =>
              frameManager.loadSourceDataFrame(
                store.connections(sou.getConnectionName),
                sou,
                Option(time))
          )
          .toList
      } else {
        Nil
      }
      val taskInput =
        TaskInput(store.connections, time, inputDepDFs ::: inputSourceDFs)

      val success = try {
        val frame = TableFunction(taskInput)
        // TODO error checking: if target should be on datalake but no frame is given
        // Note: We are responsible for all standard writing of DataFrames
        if (frame.isDefined) {
          store.tableConfig.getTargets
            .filter(t => targets.contains(t.getFormat))
            .foreach(
              tar =>
                frameManager.writeDataFrame(
                  frame.get,
                  store.connections(tar.getConnectionName),
                  tar,
                  store.tableConfig,
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

    val targetsNeeded = store.checkTargets(startTimes)
    val partitionsNeeded = targetsNeeded.map(_._1)

    val checkResult: List[Either[DependencyFailedOld, DependencySuccess]] =
      if (store.tableConfig.dependenciesSet) {
        val dependenciesByVirtual =
          store.tableConfig.getDependencies.groupBy(dep =>
            dep.getTypeEnum != DependencyType.ONEONONE)
        dependenciesByVirtual
          .map(keyDeps =>
            if (keyDeps._1) {
              val individualVirtOutcomes =
                keyDeps._2.map(checkVirtualStep(_, partitionsNeeded))
              individualVirtOutcomes.reduce(
                TableWrapper.combineDependencyChecks)
            } else {
              store.checkDependencies(keyDeps._2, partitionsNeeded)
          })
          .reduce(TableWrapper.combineDependencyChecks)
      } else {
        startTimes.map(Right(_)).toList
      }

    val tasks: mutable.ListBuffer[Future[LocalDateTime]] =
      new mutable.ListBuffer
    // Every checkResult time should be present in targetMap
    val targetMap = targetsNeeded.toMap

    checkResult
      .foreach({
        case Right(time) => {
          val runningTask = Future {
            runSingleTask(time, targetMap(time))
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

  /**
    * Utility function to create an execution context and block until runTasks is done processing the batches.
    * See [[io.qimia.uhrwerk.TableWrapper#runTasks]]
    * @param stepFunction User-defined function which combines the dependencies and processed the data
    * @param startTimes sequence of partitionTS which need to be processed
    * @param threads number of threads used by the threadpool
    */
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

  /**
    * Dependency checking for virtual-steps. When a dependency is produced by a virtual step than we need to do a
    * transformation to check its dependencies. After transforming the normal dependency checking is applied and
    * the result is transformed back into the datetime list given to checkVirtualStep
    * @param dependency virtual dependency that needs to be check
    * @param startTimes list of partitionTS that need to be checked
    * @return dependency checking result looking the same as a normal metastore dependency checking result
    */
  def checkVirtualStep(dependency: Dependency, startTimes: List[LocalDateTime])
    : List[Either[DependencyFailedOld, DependencySuccess]] = {
    // Use metaStore to check if the right batches are there

    // function for checking virtual window dependencies
    def checkWindowStep()
      : List[Either[DependencyFailedOld, DependencySuccess]] = {
      val windowSize = dependency.getPartitionCount
      val windowStartTimes = TimeTools.convertToWindowBatchList(
        startTimes,
        store.tableConfig.getBatchSizeDuration,
        windowSize)

      val windowedBatchOutcome =
        store.checkDependencies(Array(dependency), windowStartTimes)

      val booleanOutcome = windowedBatchOutcome.map({
        case Left(_)  => false
        case Right(_) => true
      })
      val windowFilter =
        TableWrapper.createFilterWithWindowList(booleanOutcome, windowSize)
      val filteredOutcome = TableWrapper.applyWindowFilter(windowedBatchOutcome,
                                                           windowFilter,
                                                           dependency.getFormat)
      // TODO: Change to name in-case we switch from using the paths to using identifiers/names

      TimeTools.cleanWindowBatchList(filteredOutcome, windowSize)
    }

    // function for checking virtual aggregate dependencies
    def checkAggregateStep()
      : List[Either[DependencyFailedOld, DependencySuccess]] = {
      // TODO: Doesn't check if the given duration (dep.getPartitionSizeDuration) agrees with this division
      val smallerStartTimes = TimeTools.convertToSmallerBatchList(
        startTimes,
        store.tableConfig.getBatchSizeDuration,
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
                                  store.tableConfig.getBatchSizeDuration,
                                  dependency.getPartitionCount,
                                  foundSmallBatches)
        .toSet

      startTimes.map(datetime =>
        if (foundBigBatches.contains(datetime)) {
          Right(datetime)
        } else {
          Left((datetime, Set(dependency.getFormat)))
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
