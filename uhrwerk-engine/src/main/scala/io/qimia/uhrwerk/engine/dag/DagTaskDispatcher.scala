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
      val futures = task.table.runTasks(task.partitions.toList)
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
        task.table.runTasks(task.partitions.toList).map(res => (res, taskWIndex._2))
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
