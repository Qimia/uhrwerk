package io.qimia.uhrwerk.engine.tools

import java.time.Duration

import io.qimia.uhrwerk.common.framemanager.BulkDependencyResult
import io.qimia.uhrwerk.common.metastore.dependency.{
  TablePartitionResult,
  TablePartitionResultSet
}
import io.qimia.uhrwerk.engine.Environment.TableIdent

import scala.collection.mutable

object DependencyHelper {

  /**
    * Create TableIdentity for a bulk dependency
    * @param bulkDependency a bulk dependencyResult
    * @return a table identifier
    */
  def extractTableIdentity(bulkDependency: BulkDependencyResult): TableIdent =
    TableIdent(bulkDependency.dependency.getArea,
               bulkDependency.dependency.getVertical,
               bulkDependency.dependency.getTableName,
               bulkDependency.dependency.getVersion)

  /**
    * Group together continuous sequential groups of resolved TablePartitionResult
    * @param in full TablePartitionResultSet with all the results
    * @param maxSize maximum size of the grouped together results
    * @return array with grouped together TablePartitionResult objects
    */
  def createTablePartitionResultGroups(
      in: TablePartitionResultSet,
      partitionSize: Duration,
      maxSize: Int): List[Array[TablePartitionResult]] = {
    val groups = TimeHelper.groupSequentialIncreasing(in.getResolvedTs,
                                                      partitionSize,
                                                      maxSize)
    var queue = in.getResolved
    val res: mutable.ListBuffer[Array[TablePartitionResult]] =
      new mutable.ListBuffer
    groups.foreach(num => {
      val (group, rest) = queue.splitAt(num)
      res += group
      queue = rest
    })
    res.toList
  }

  /**
    * Go from a TablePartitionResult for a list of partitions, to a BulkDependencyResult for a list of Dependencies
    * (facilitating grouping multiple batches for each dependency and assumes that they are combinable)
    * @param partitionResults Array of TablePartitionResults which need to be transformed together
    * @return Array of BulkDependencyResult, one for each dependency
    */
  def extractBulkDependencyResult(partitionResults: Array[TablePartitionResult])
    : List[BulkDependencyResult] = {
    val lbuffer = new mutable.ListBuffer[BulkDependencyResult]

    val firstResultDependencies = partitionResults.head.getResolvedDependencies
    val numDependencies = firstResultDependencies.size
    for (i <- 0 until numDependencies) {
      val tsAndPartitions = partitionResults.map(res => {
        val partTS = res.getPartitionTs
        val depRes = res.getResolvedDependencies()(i)
        val partitions = depRes.getPartitions
        (partTS, partitions)
      })
      lbuffer += BulkDependencyResult(
        tsAndPartitions.map(_._1),
        firstResultDependencies(i).getDependency,
        firstResultDependencies(i).getConnection,
        tsAndPartitions
          .flatMap(_._2).distinct  // combine all partitions into one array and remove doubles
      )
    }
    lbuffer.toList
  }

}
