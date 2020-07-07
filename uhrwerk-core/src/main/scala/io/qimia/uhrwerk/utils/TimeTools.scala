package io.qimia.uhrwerk.utils

import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ListBuffer

object TimeTools {

  // convert a datetime string to a datetime object
  def convertTSToTimeObj(date: String): LocalDateTime =
    LocalDateTime.parse(date, DateTimeFormatter.ISO_LOCAL_DATE_TIME)

  // convert a datetime object to a string representation
  def convertTSToString(date: LocalDateTime): String =
    date.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

  // Go from a string representation of a duration to a duration object
  def convertDurationToObj(duration: String): Duration = {
    duration.toCharArray.last match {
      case 'm' =>
        Duration.ofMinutes(duration.substring(0, duration.length() - 1).toInt)
      case 'h' =>
        Duration.ofHours(duration.substring(0, duration.length() - 1).toInt)
      case 'd' =>
        Duration.ofDays(duration.substring(0, duration.length() - 1).toInt)
    }
  }

  // go from a duration object to a string representation
  def convertDurationToStr(duration: Duration): String = {
    val totalMinutes = duration.toMinutes
    if ((totalMinutes % 60) == 0) {
      val totalHours = totalMinutes / 60
      if ((totalHours % 24) == 0) {
        val totalDays = totalHours / 24
        s"${totalDays}d"
      } else {
        s"${totalHours}h"
      }
    } else {
      s"${totalMinutes}m"
    }
  }

  // Go from range to a list of dates (with a given batchsize) (start inclusive end exclusive)
  def convertRangeToBatch(startDate: LocalDateTime,
                          endDate: LocalDateTime,
                          batchSize: Duration): List[LocalDateTime] = {

    val batches = new ListBuffer[LocalDateTime]()

    var tempStart = startDate
    var active = true
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
  def convertToSmallerBatchList(largeBatchList: List[LocalDateTime],
                                runBatchSize: Duration,
                                divideBy: Int): List[LocalDateTime] = {
    val smallerDuration = runBatchSize.dividedBy(divideBy)
    val durationAdditions = (0 until divideBy).toList
      .map(x => smallerDuration.multipliedBy(x.toLong))
    for (batch <- largeBatchList;
         durationAdd <- durationAdditions)
      yield { batch.plus(durationAdd) }
  }

  // See for a list of larger batches if a list of smaller batches (with a given big-batch-duration and divideBy)
  // which of those large batches have all the smaller batches present
  def filterBySmallerBatchList(
      largeBatchList: List[LocalDateTime],
      runBatchSize: Duration,
      divideBy: Int,
      smallBatches: Set[LocalDateTime]): List[LocalDateTime] = {
    val smallerDuration = runBatchSize.dividedBy(divideBy)
    val durationAdditions = (0 until divideBy).toList
      .map(x => smallerDuration.multipliedBy(x.toLong))

    largeBatchList
      .map(ts => durationAdditions.map(dur => ts.plus(dur))) // List of list of smaller subbatches
      .filter(smallTimeList => {
        if (smallTimeList.forall(smallTS => smallBatches.contains(smallTS))) {
          true // if subbatches are all present continue
        } else {
          false
        }
      })
      .map(_.head) // Convert back to big batches
  }

}
