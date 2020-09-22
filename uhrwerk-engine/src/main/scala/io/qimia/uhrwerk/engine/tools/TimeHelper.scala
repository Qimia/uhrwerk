package io.qimia.uhrwerk.engine.tools

import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.common.model.PartitionUnit

import scala.collection.mutable

object TimeHelper {

  /**
    * Group together partitions when they are sequential upto a certain max number per group.
    * If there are gaps then they can't be grouped.
    *
    * @param in            total sequency of LocalDateTime that could be grouped together
    * @param partitionSize Duration between two partitions
    * @param maxSize       max number of partitions in a
    * @return Array of int showing the size of the different groups
    */
  def groupSequentialIncreasing(in: Seq[LocalDateTime], partitionSize: Duration, maxSize: Int): Array[Int] = {
    val outArray: mutable.ArrayBuffer[Int] =
      new mutable.ArrayBuffer[Int]

    var singleGroup                   = 0
    var lastDt: Option[LocalDateTime] = Option.empty
    for (dt <- in) {
      if (singleGroup == 0) {
        singleGroup += 1
        lastDt = Option(dt)
      } else if (singleGroup >= maxSize) {
        outArray.append(singleGroup)
        singleGroup = 1
        lastDt = Option(dt)
      } else {
        val gap = Duration.between(lastDt.get, dt)
        if (gap == partitionSize) {
          singleGroup += 1
          lastDt = Option(dt)
        } else {
          outArray.append(singleGroup)
          singleGroup = 1
          lastDt = Option(dt)
        }
      }
    }
    outArray.append(singleGroup)
    outArray.toArray
  }

  /**
    * Bulk together a list of partition times given a duration and a maximum bulk size
    * @param times LocalDateTime list that needs to be split
    * @param partitionSize Duration between datetime (table partition size)
    * @param maxSize maximum count in one bulk-list
    * @return list of a bulk-list of localdatetimes
    */
  def createPartitionBulkGroups(times: List[LocalDateTime],
                                partitionSize: Duration,
                                maxSize: Int): List[List[LocalDateTime]] = {
    val groups = TimeHelper.groupSequentialIncreasing(times, partitionSize, maxSize)
    var queue  = times
    val res: mutable.ListBuffer[List[LocalDateTime]] =
      new mutable.ListBuffer
    groups.foreach(num => {
      val (group, rest) = queue.splitAt(num)
      res += group
      queue = rest
    })
    res.toList
  }

  /**
    * Create duration object out of unit and a size (count)
    * @param unit unit of time (ie. weeks, days, hours, minutes)
    * @param size size / count (ie. 10 days or 30 minutes)
    * @return Duration object for use with LocalDateTime
    */
  def convertToDuration(unit: PartitionUnit, size: Int): Duration = {
    unit match {
      case PartitionUnit.WEEKS   => Duration.ofDays(size * 7)
      case PartitionUnit.DAYS    => Duration.ofDays(size)
      case PartitionUnit.HOURS   => Duration.ofHours(size)
      case PartitionUnit.MINUTES => Duration.ofMinutes(size)
    }
  }

}
