package io.qimia.uhrwerk.engine.dag

import java.util.concurrent.{ExecutorService, Executors}

import scala.util.{Failure, Success}
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.concurrent.duration.Duration

object DagTaskDispatcher {
  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
    * Execute the taskqueue
    *
    * @param tasks a list of tables that need to be processed and their partition-times
    */
  def runTasks(tasks: Seq[DagTask]): Unit = {
    val executor                                            = Executors.newSingleThreadExecutor()
    implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    def procTask(task: DagTask): Unit = {
      val futures = task.table.runTasks(task.partitions.toArray)
      val result  = Await.result(Future.sequence(futures), duration.Duration(24, duration.HOURS))
      if (!result.forall(res => res)) {
        logger.error(s"Task table ${task.table.wrappedTable.getName} failed for ${task.partitions}")
      }
    }

    val deduplicatedTasks = DagTaskBuilder.distinctDagTasks(tasks)
    deduplicatedTasks.foreach(procTask)
    executor.shutdown()
  }

  def getExecutor(threads: Int): ExecutorService = {
    if (threads < 2) {
      Executors.newSingleThreadExecutor()
    } else {
      Executors.newFixedThreadPool(threads)
    }
  }

  /**
    *
    */
  private val recursiveRunLock = ""

  /**
    *
    * @param executionContext
    * @param parentFuture
    * @param taskKey
    * @param allTasks
    * @param taskScheduledOrStarted
    * @return
    */
  def triggerTasksRecursively(implicit executionContext: ExecutionContextExecutor,
                              parentFuture: Future[Boolean],
                              taskKey: DT2Key,
                              allTasks: Map[DT2Key, DT2],
                              taskScheduledOrStarted: mutable.Set[DT2Key]): List[Future[Boolean]] = {
    logger.info(s"Recursing $taskKey")
    val task = allTasks(taskKey)

    // For this table, schedule all its partitions to run in  a chain.
    val parentFutureResult = parentFuture.map { res =>
      {
        recursiveRunLock.synchronized({
          val taskAlreadyScheduled = taskScheduledOrStarted contains taskKey
          val thereAreFailedTasks = false
          val state               = (task.missingDependencies.isEmpty, res, taskAlreadyScheduled, thereAreFailedTasks)
          val missDeps = task.missingDependencies.map(_.ident.asPath).toList.mkString(";")
          state match {
            case (true, true, false, false) => {
              logger.info("The task is ready to run.")
              logger.debug(f"We mark the task ${taskKey.ident.asPath} as running.")
              taskScheduledOrStarted += taskKey}
            case (false, _, _, _) =>
              logger.info(s"Task $taskKey won't be scheduled yet as it has missing dependencies: $missDeps.")
            case (_, false, _, _) =>
              logger.info(s"Task $taskKey won't be started as one of the predecessor dependencies has failed.")
            case (_, _, true, _) =>
              logger.error(s"Task $taskKey won't be started as the task is already triggered " +
                s"by some other task. This shouldn't have happened.")
            case x =>
              logger.error("This should be caught by something else"); logger.info(x);
          }
          state
        })
      }
    }
    val tableFinishedFuture = parentFutureResult.map{
      case (true, true, false, false) => {
        logger.info(s"Running a partition for $taskKey")
        val subTasksToRun = task.tableWrapper.getTaskRunners(task.partitions.toArray)
        val taskResult = subTasksToRun.forall(_.apply())
        logger.info(s"Ran partitions for $taskKey, with success $taskResult")
        (true, true, false, !taskResult)
      }
      case (missingDependency, previousTaskFailed, taskAlreadyScheduled, true) => {
        logger.error(f"Skipping the partition for $taskKey as one of its other partitions failed.")
        (missingDependency, previousTaskFailed, taskAlreadyScheduled, true)
      }
      case x => x
    }.map {
      case (true, true, false, false) => {
        // Task finished successfully
        recursiveRunLock.synchronized({
          logger.info(f"The task with key: $taskKey is finished.")
          // Inform the upstream dependencies that this task is done
          task.upstreamDependencies.foreach(upstreamTaskKey => {
            val upstreamTask = allTasks(upstreamTaskKey)
            logger.warn(s"Removing the dependency ${taskKey.ident.asPath} from ${upstreamTaskKey.ident.asPath}")
            upstreamTask.missingDependencies.remove(taskKey)
          })
        })
        true
      }
      case x => {
        false
      }
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

  /**
    * Starts the recursive function @method triggerTasksRecursively
    * Starting from the tasks with no missing dependencies, all will be processed.
    * @param tasks List of all tasks in the complete DAG
    * @param threads
    */
  def runTasksParallelWithFullDAGGeneration(tasks: Map[DT2Key, DT2], threads: Int): Unit = {
    val executor                                            = getExecutor(threads)
    implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
    val futures = tasks
      .filter(_._2.missingDependencies.isEmpty)
      .keys
      .toList
      .flatMap(readyTask =>
        triggerTasksRecursively(executionContext, Future {
          true
        }, readyTask, tasks, mutable.Set.empty))
    //Thread.sleep(10000000)
    futures.foreach(Await.result(_, Duration.Inf))
  }

  /**
    * Execute the taskqueue in parallel
    *
    * @param tasks a list of tables that need to be processed and their partition-times
    */
  def runTasksParallel(tasks: Seq[DagTask], threads: Int): Unit = {
    val executor                                            = getExecutor(threads)
    implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    def procTasks(tasks: Seq[DagTask]): Unit = {
      val futures = tasks.zipWithIndex.flatMap(taskWIndex => {
        val task = taskWIndex._1
        task.table.runTasks(task.partitions.toArray).map(res => (res, taskWIndex._2))
        // Add a reference to which call resulted in the failed / succeeded DagTask
      })
      val result =
        Await.result(Future.sequence(futures.map(_._1)), duration.Duration(24, duration.HOURS)).zip(futures.map(_._2))
      result.foreach(res => {
        if (!res._1) {
          logger.error(s"Task table ${tasks(res._2).table.wrappedTable.getName} failed for ${tasks(res._2).partitions}")
        }
      })
    }

    val deduplicatedTasks = DagTaskBuilder.distinctDagTasks(tasks)
    val groupedTasks = deduplicatedTasks
      .groupBy(dagTask => dagTask.dagDept)
      .toList
      .sortBy(taskGroup => taskGroup._1)(Ordering[Int].reverse)
    groupedTasks.foreach(tup => procTasks(tup._2))
    executor.shutdown()
  }

}
