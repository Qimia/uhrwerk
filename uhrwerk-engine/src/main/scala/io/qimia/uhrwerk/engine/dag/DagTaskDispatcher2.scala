package io.qimia.uhrwerk.engine.dag

import java.util.concurrent.{ExecutorService, Executors}

import org.apache.log4j.Logger

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object DagTaskDispatcher2 {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def getExecutor(threads: Int): ExecutorService = {
    if (threads < 2) {
      Executors.newSingleThreadExecutor()
    } else {
      Executors.newFixedThreadPool(threads)
    }
  }

  /**
    *
    * @param executionContext
    * @param parentFuture
    * @param taskKey
    * @param allTasks
    * @param taskScheduledOrStarted
    * @return
    */
  private def triggerTasksRecursively(
      implicit executionContext: ExecutionContextExecutor,
      parentFuture: Future[(Option[DagTask2Key], Boolean)],
      taskKey: DagTask2Key,
      allTasks: Map[DagTask2Key, DagTask2],
      taskScheduledOrStarted: mutable.Set[DagTask2Key]): List[Future[(Option[DagTask2Key], Boolean)]] = {
    logger.info(s"Recursing ${taskKey.identifiableString}")
    val task = allTasks(taskKey)

    // For this table, schedule all its partitions to run in  a chain.
    val parentFutureResult = parentFuture.map {
      case (ancestorKey, ancestorsSuccess) => { // check if the task is in runnable state
        allTasks.synchronized({
          val ancestorName = ancestorKey match {
            case Some(ancestor) => ancestor.identifiableString
            case None           => "root"
          }
          val taskAlreadyScheduled = taskScheduledOrStarted contains taskKey
          val thereAreFailedTasks  = false
          val state                = (task.missingDependencies.isEmpty, ancestorsSuccess, taskAlreadyScheduled, thereAreFailedTasks)
          val missDeps             = task.missingDependencies.map(_.ident.asPath).toList.mkString("; ")
          state match {
            case (true, true, false, false) => {
              logger.info(f"The task ${taskKey.ident.asPath} is ready to run. We mark it as running.")
              taskScheduledOrStarted += taskKey
            }
            case (_, false, _, _) =>
              logger.warn(
                s"Task ${taskKey.identifiableString} won't be started yet as its ancestor $ancestorName's dependencies have failed" +
                  s" or one of its ancestors has beeen skipped for missing dependencies. We'll try again" +
                  s" automatically once the dependencies are fulfilled.")
            case (false, _, _, _) =>
              logger.debug(
                s"Task ${taskKey.identifiableString} won't be scheduled yet as it has missing dependencies: '$missDeps'.")
            case (_, _, true, _) =>
              logger.error(
                s"Task ${taskKey.identifiableString} won't be started as the task is already triggered " +
                  s"by some other task. This shouldn't have happened.")
            case x =>
              logger.error("This should be caught by something else"); logger.info(x);
          }
          state
        })
      }
    }
    val tableFinishedFuture = parentFutureResult
      .flatMap {
        // If the state condition is fulfilled, run the task
        case (true, true, false, false) => {
          logger.info(s"${taskKey.ident.asPath}: starting to run bulk(s in parallel).")
          val subTasksToRun = task.tableWrapper.getTaskRunners(task.partitions.toArray)
          val taskResult =
            Future.sequence(subTasksToRun.map(subTask => Future { (true, true, false, false, subTask.apply()) }))
          taskResult
        }
        case (missingDependency, previousTaskFailed, taskAlreadyScheduled, true) => {
          logger.error(f"${taskKey.ident.asPath}: a task failed as one of its other partitions failed.")
          Future { ((missingDependency, previousTaskFailed, taskAlreadyScheduled, true, true) :: Nil) }
        }
        case x => Future { ((x._1, x._2, x._3, x._4, true) :: Nil) }
      }
      .map(x => (x.head._1, x.head._2, x.head._3, x.head._4, x.map(_._5)))
      .map(x => x.copy(_5 = x._5.count(!_))) // Count the number of failures
      .map {
        case (true, true, false, false, 0) =>
          // Task finished successfully
          allTasks.synchronized({
            logger.info(f"${taskKey.identifiableString} is finished.")
            // Inform the upstream dependencies that this task is done
            task.upstreamDependencies.foreach(upstreamTaskKey => {
              val upstreamTask = allTasks(upstreamTaskKey)
              logger.warn(
                s"Removing the dependency ${taskKey.identifiableString} from ${upstreamTaskKey.identifiableString}")
              upstreamTask.missingDependencies.remove(taskKey)
            })
          })
          (Some(taskKey), true)
        case (true, true, false, false, failureCount) =>
          logger.warn(f"${taskKey.ident.asPath}: $failureCount tasks have failed.")
          (Some(taskKey), false)
        case (false, true, false, false, _) =>
          (Some(taskKey), true)
        case _ => (Some(taskKey), false)
      }
    val upstreamFutures = task.upstreamDependencies.toList.flatMap { nextTaskKey =>
      triggerTasksRecursively(
        executionContext,
        tableFinishedFuture,
        nextTaskKey,
        allTasks,
        taskScheduledOrStarted
      )
    }
    tableFinishedFuture :: upstreamFutures
  }

  def runTasks(tasks: Map[DagTask2Key, DagTask2], threads: Int): Unit = {
    val executor                                            = getExecutor(threads)
    implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
    val futures = tasks
      .filter(_._2.missingDependencies.isEmpty)
      .keys
      .toList
      .flatMap(readyTask =>
        triggerTasksRecursively(executionContext, Future {
          (None, true)
        }, readyTask, tasks, mutable.Set.empty))
    futures.foreach(Await.result(_, Duration.Inf))
  }
}
