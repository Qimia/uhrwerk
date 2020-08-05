package io.qimia.uhrwerk.utils

import java.time.{Duration, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter

import scala.collection.mutable.ListBuffer

object TimeTools {

  // convert a datetime string to a datetime object
  def convertTSToTimeObj(date: String): LocalDateTime =
    LocalDateTime.parse(date, DateTimeFormatter.ISO_LOCAL_DATE_TIME)

  // convert a datetime object to a string representation
  def convertTSToString(date: LocalDateTime): String =
    date.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

  // TODO: Durations now have to be exact minutes or hours or days (what if it isn't??)

  def getRangeFromAggregate(ts: LocalDateTime, duration: String, partitionCount: Int): (LocalDateTime, LocalDateTime) = {
    val durationObj = convertDurationToObj(duration)
    val multipliedDuration = durationObj.multipliedBy(partitionCount)

    // checking for validity
    // TODO add months
    val isHour = multipliedDuration.minusHours(1).getSeconds == 0
    val isDay = multipliedDuration.minusDays(1).getSeconds == 0
    if (!isHour && !isDay) {
      throw new UnsupportedOperationException("A wrong aggregate specified")
    }

    if (isHour) {
      val tsHour = ts.minusMinutes(ts.getMinute).minusSeconds(ts.getSecond)
      (tsHour, tsHour.plusHours(1))
    } else { //isDay
      val tsDay = ts.minusHours(ts.getHour).minusMinutes(ts.getMinute).minusSeconds(ts.getSecond)
      (tsDay, tsDay.plusDays(1))
    }
  }

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
    // TODO: Doesn't handle combinations (1h30m) yet.
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

  // Convert a localtime to a batch number (for when batches are smaller than an hour)
  def getBatchInHourNumber(time: LocalTime, duration: Duration): Long = {
    val minuteHand = time.getMinute
    val minuteDuration = duration.toMinutes
    minuteHand / minuteDuration
  }

  /**
   * Go from range to a list of dates (with a given batchsize) (start inclusive end exclusive)
   * @param startDate
   * @param endDate
   * @param batchSize
   * @return
   */
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

  // Adds windowSize - 1 batches to the front of the batchList for windowed dependencies
  def convertToWindowBatchList(largeBatchList: List[LocalDateTime],
                               stepBatchSize: Duration,
                               windowSize: Int): List[LocalDateTime] = {
    val timeSubtractions = (-windowSize + 1 until 0).map(stepBatchSize.multipliedBy(_)).map(largeBatchList.head.plus(_))
    timeSubtractions ++: largeBatchList
  }

  // Remove added window elements from the list (reverse of creating the window list)
  def cleanWindowBatchList[T](windowedBatchList: List[T], windowSize: Int): List[T] = windowedBatchList.drop(windowSize - 1)


  def divisibleBy(big: Duration, small: Duration): Boolean = big.toMinutes % small.toMinutes == 0

  @scala.annotation.tailrec
  def leftPad(s: String): String = {
    if (s.length < 2) {
      leftPad("0" + s)
    } else {
      s
    }
  }

  /**
   * Convert batch start timestamp to postfix needed for reading single batch
   *
   * @param date DateTime
   * @param duration Duration
   * @return String postfix
   */
  def dateTimeToPostFix(date: LocalDateTime, duration: Duration): String = {
    val year = date.getYear
    val month = leftPad(date.getMonthValue.toString)
    val day = leftPad(date.getDayOfMonth.toString)
    if (duration.toHours >= Duration.ofDays(1).toHours) {
      return s"${year}-${month}-${day}"
    }
    val hour = leftPad(date.getHour.toString)
    if (duration.toMinutes >= Duration.ofHours(1).toMinutes) {
      return s"${year}-${month}-${day}-${hour}"
    }
    val batch = TimeTools.getBatchInHourNumber(date.toLocalTime, duration).toString
    s"${year}-${month}-${day}-${hour}-${batch}"
  }

  /**
   * Convert batch start timestamp to date
   *
   * @param date DateTime
   * @return String date
   */
  def dateTimeToDate(date: LocalDateTime): String = {
    val year = date.getYear
    val month = leftPad(date.getMonthValue.toString)
    val day = leftPad(date.getDayOfMonth.toString)
    s"${year}-${month}-${day}"
  }
}
