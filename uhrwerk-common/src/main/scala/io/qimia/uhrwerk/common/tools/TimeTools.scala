package io.qimia.uhrwerk.common.tools

import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ListBuffer

object TimeTools {

  /**
   * Calculates an upper bound for a given partition timestamp, unit and size.
   *
   * @param partitionTs   Partition timestamp.
   * @param partitionSize Partition size.
   * @param partitionUnit Partition unit.
   * @return Upper bound.
   */
  def addPartitionSizeToTimestamp(
                                   partitionTs: LocalDateTime,
                                   partitionSize: Int,
                                   partitionUnit: PartitionUnit
                                 ): LocalDateTime = {
    if (partitionTs == null) {
      throw new IllegalArgumentException("Partition timestamp cannot be null")
    }
    if (partitionSize == 0) {
      throw new IllegalArgumentException("Partition size cannot be null/0")
    }
    if (partitionUnit == null) {
      throw new IllegalArgumentException("Partition unit cannot be null")
    }

    partitionUnit match {
      case PartitionUnit.MINUTES => partitionTs.plusMinutes(partitionSize)
      case PartitionUnit.HOURS => partitionTs.plusHours(partitionSize)
      case PartitionUnit.DAYS => partitionTs.plusDays(partitionSize)
      case PartitionUnit.WEEKS => partitionTs.plusDays(7 * partitionSize)
    }
  }

  case class DurationTuple(count: Int, durationUnit: PartitionUnit)

  /**
   * Converts a datetime object to a string representation.
   *
   * @param date Timestamp.
   * @return String representation.
   */
  def convertTSToString(date: LocalDateTime): String =
    date.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

  /**
   * Converts a datetime object to a UTC string representation.
   *
   * @param date Timestamp.
   * @return String representation.
   */
  def convertTSToUTCString(date: LocalDateTime): String =
    Timestamp
      .valueOf(
        date
          .atZone(ZoneId.systemDefault)
          .withZoneSameInstant(ZoneOffset.UTC)
          .toLocalDateTime
      )
      .toString

  /**
    * Go from range to a list of dates (with a given batchsize) (start inclusive end exclusive)
    *
    * @param startDate
    * @param endDate
    * @param batchSize
    * @return
    */
  def convertRangeToBatch(
      startDate: LocalDateTime,
      endDate: LocalDateTime,
      batchSize: Duration
  ): List[LocalDateTime] = {

    val batches = new ListBuffer[LocalDateTime]()

    var tempStart = startDate
    var active    = true
    while (active) {
      var tempEnd = tempStart.plus(batchSize)
      if (tempStart.isAfter(startDate) || tempStart.isEqual(startDate)) {
        if (tempEnd.isBefore(endDate) || tempEnd.isEqual(endDate)) {
          // Only add a date once a full duration passed (if it doesn't fit return an empty list)
          batches += tempStart
          tempStart = tempEnd
          tempEnd = tempStart.plus(batchSize)
        } else
          active = false
      } else
        active = false
    }
    batches.toList
  }

  // Create a small batch list from a more course one (for taking in smaller dependencies)
  // Assumes the large-batches are already ordered in time
  def convertToSmallerBatchList(
      largeBatchList: List[LocalDateTime],
      runBatchSize: Duration,
      divideBy: Int
  ): List[LocalDateTime] = {
    val smallerDuration = runBatchSize.dividedBy(divideBy)
    val durationAdditions = (0 until divideBy).toList
      .map(x => smallerDuration.multipliedBy(x.toLong))
    for (
      batch <- largeBatchList;
      durationAdd <- durationAdditions
    )
      yield {
        batch.plus(durationAdd)
      }
  }

  // Adds windowSize - 1 batches to the front of the batchList for windowed dependencies
  def convertToWindowBatchList(
      largeBatchList: List[LocalDateTime],
      stepBatchSize: Duration,
      windowSize: Int
  ): List[LocalDateTime] = {
    val timeSubtractions = (-windowSize + 1 until 0)
      .map(stepBatchSize.multipliedBy(_))
      .map(largeBatchList.head.plus(_))
    timeSubtractions ++: largeBatchList
  }

  @scala.annotation.tailrec
  def leftPad(s: String): String = {
    if (s.length < 2) {
      leftPad("0" + s)
    } else {
      s
    }
  }

  /**
   * Assigns a timestamp into its aggregate based on a partition unit and size.
   *
   * @param partitionTS   An array of aggregate timestamps (e.g. 2020-09-01, 2020-09-04).
   * @param ts            The timestamp to assign. (e.g. 2020-09-03).
   * @param partitionUnit Partition Unit (e.g. DAYS).
   * @param partitionSize Partition Size (e.g. 3).
   * @return One of the partitionTS timestamps (e.g. in this case 2020-09-01).
   */
  def getAggregateForTimestamp(
      partitionTS: Array[LocalDateTime],
      ts: LocalDateTime,
      partitionUnit: String,
      partitionSize: Int
  ): LocalDateTime = {
    for (p <- partitionTS) {
      val diffDuration = Duration.between(p, ts)
      val diff = partitionUnit match {
        case "DAYS" => diffDuration.toDays
        case "HOURS" => diffDuration.toHours
        case "MINUTES" => diffDuration.toMinutes
        case "WEEKS" => diffDuration.toDays / 7
        case _ => 0
      }
      if (diff >= 0 && diff < partitionSize) {
        return p
      }
    }

    ts
  }

  /**
   * Calculates the lower bound of the current timestamp.
   * It also needs one partitionTs to see when exactly that partition starts and also partitionUnit and partitionSize.
   *
   * @param partitionTs   An example partition timestamp.
   * @param nowTs         The current timestamp.
   * @param partitionUnit Partition Unit.
   * @param partitionSize Partition Size.
   * @return A timestamp that fits into the partition style and is smaller or equal to [[nowTs]].
   */
  def lowerBoundOfCurrentTimestamp(
                                    partitionTs: LocalDateTime,
                                    nowTs: LocalDateTime,
                                    partitionUnit: PartitionUnit,
                                    partitionSize: Int
                                  ): LocalDateTime = {
    val diff = Duration.between(partitionTs, nowTs)
    val days = diff.toDays
    val hours = diff.toHours
    val minutes = diff.toMinutes

    partitionUnit match {
      case PartitionUnit.WEEKS => partitionTs.plusDays(days - (days % (7 * partitionSize)))
      case PartitionUnit.DAYS => partitionTs.plusDays(days - (days % partitionSize))
      case PartitionUnit.HOURS => partitionTs.plusHours(hours - (hours % partitionSize))
      case PartitionUnit.MINUTES => partitionTs.plusMinutes(minutes - (minutes % partitionSize))
    }
  }
}
