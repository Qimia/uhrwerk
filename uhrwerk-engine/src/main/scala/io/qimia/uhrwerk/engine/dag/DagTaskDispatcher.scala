package io.qimia.uhrwerk.engine.dag

import java.util.concurrent.Executors

import scala.concurrent._

object DagTaskDispatcher {

  /**
    * Execute the taskqueue
    * @param tasks a list of tables that need to be processed and their partition-times
    */
  def runTasks(tasks: Seq[DagTask]): Unit = {
    val executor                                            = Executors.newSingleThreadExecutor()
    implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    def procTask(task: DagTask) = {
      val futures = task.table.runTasks(task.partitions.toArray)
      val result  = Await.result(Future.sequence(futures), duration.Duration(24, duration.HOURS))
      if (!result.forall(res => res)) {
        System.err.println(s"Task table ${task.table.wrappedTable.getName} failed for ${task.partitions}")
      }
    }
    val deduplicatedTasks = DagTaskBuilder.distinctDagTasks(tasks)
    deduplicatedTasks.foreach(procTask)
    executor.shutdown()
  }

  /**
    * Execute the taskqueue in parallel
    * @param tasks a list of tables that need to be processed and their partition-times
    */
  def runTasksParallel(tasks: Seq[DagTask], threads: Int): Unit = {
    val executor = if (threads < 2) {
      Executors.newSingleThreadExecutor()
    } else {
      Executors.newFixedThreadPool(threads)
    }
    implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

    def procTasks(tasks: Seq[DagTask]) = {
      val futures = tasks.zipWithIndex.flatMap(taskWIndex => {
        val task = taskWIndex._1
        task.table.runTasks(task.partitions.toArray).map(res => (res, taskWIndex._2))
        // Add a reference to which call resulted in the failed / succeeded DagTask
      })
      val result =
        Await.result(Future.sequence(futures.map(_._1)), duration.Duration(24, duration.HOURS)).zip(futures.map(_._2))
      result.foreach(res => {
        if (!res._1) {
          System.err.println(
            s"Task table ${tasks(res._2).table.wrappedTable.getName} failed for ${tasks(res._2).partitions}")
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
