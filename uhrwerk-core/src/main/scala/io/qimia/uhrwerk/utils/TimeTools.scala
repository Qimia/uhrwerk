package io.qimia.uhrwerk.utils

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime, LocalTime}

import com.mysql.cj.exceptions.WrongArgumentException
import io.qimia.uhrwerk.backend.model.BatchTemporalUnit

import scala.collection.mutable.ListBuffer

object TimeTools {
  /**
   * Checks whether the duration size is days.
   *
   * @param batchDuration Duration to check.
   * @return True/False
   */
  def isDurationSizeDays(batchDuration: Duration): Boolean = {
    convertDurationToStr(batchDuration).contains("d")
  }

  /**
   * Converts a duration to a backend BatchTemporalUnit.
   *
   * @param duration Duration to convert.
   * @return Option[BatchTemporalUnit]
   */
  def convertDurationToBatchTemporalUnit(duration: Duration): Option[BatchTemporalUnit] = {
    val mappings = Map("m" -> BatchTemporalUnit.MINUTES,
      "h" -> BatchTemporalUnit.HOURS,
      "d" -> BatchTemporalUnit.DAYS,
      "l" -> BatchTemporalUnit.MONTHS, // todo check if named correctly - "l" as Luna
      "y" -> BatchTemporalUnit.YEARS)

    mappings collectFirst {
      case (str, v) if convertDurationToStr(duration) contains str => v
    }
  }

  /**
   * Converts a string duration into a batch size as integer.
   *
   * @param duration String duration.
   * @return Batch size as Int.
   */
  def convertDurationToBatchSize(duration: String): Int = {
    duration.slice(0, duration.length - 1).toInt
  }

  // convert a datetime string to a datetime object
  def convertTSToTimeObj(date: String): LocalDateTime =
    LocalDateTime.parse(date, DateTimeFormatter.ISO_LOCAL_DATE_TIME)

  // convert a datetime object to a string representation
  def convertTSToString(date: LocalDateTime): String =
    date.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

  // TODO: Durations now have to be exact minutes or hours or days (what if it isn't??)

  /**
   * Returns min and max (exclusive) timestamps for a specified aggregate.
   * E.g. with input (LocalDateTime.of(2020, 1, 1, 8, 45, 23), "15m", 4) returns
   * (LocalDateTime.of(2020, 1, 1, 8, 0), LocalDateTime.of(2020, 1, 1, 9, 0))
   * It is only allowed to go to a full hour, full day or a full month.
   * The underlying batch size can differ though, e.g. going both from "15m" and "30m" to "1h" is feasible.
   *
   * @param ts             TimeStamp that is taken as the basis of the aggregate.
   * @param duration       Duration as string, e.g. "15m" or "1h".
   * @param partitionCount The multiplier of the duration - the result needs to give a full aggregate.
   *                       E.g. "15m" * 5 is not valid.
   * @return A tuple with min and max timestamps of the aggregate.
   * @throws UnsupportedOperationException if the aggregate is not valid, e.g. "1h" * 7 -> not a full day
   */
  def getRangeFromAggregate(ts: LocalDateTime, duration: String, partitionCount: Int): (LocalDateTime, LocalDateTime) = {
    val durationObj = convertDurationToObj(duration)
    val multipliedDuration = durationObj.multipliedBy(partitionCount)

    // checking for validity
    val isHour = multipliedDuration.minusHours(1).getSeconds == 0
    val isDay = multipliedDuration.minusDays(1).getSeconds == 0
    val correctNumberOfDays = ts.getMonth.length(((ts.getYear % 4 == 0) && (ts.getYear % 100 != 0)) || (ts.getYear % 400 == 0))
    val isMonth = multipliedDuration.minusDays(correctNumberOfDays).getSeconds == 0
    if (!isHour && !isDay && !isMonth) {
      throw new UnsupportedOperationException("A wrong aggregate specified")
    }

    if (isHour) {
      val tsHour = ts.minusMinutes(ts.getMinute).minusSeconds(ts.getSecond)
      (tsHour, tsHour.plusHours(1))
    } else if (isDay) {
      val tsDay = ts.minusHours(ts.getHour).minusMinutes(ts.getMinute).minusSeconds(ts.getSecond)
      (tsDay, tsDay.plusDays(1))
    } else { // isMonth
      val tsMonth = ts.minusDays(ts.getDayOfMonth - 1).minusHours(ts.getHour).minusMinutes(ts.getMinute).minusSeconds(ts.getSecond)
      (tsMonth, tsMonth.plusMonths(1))
    }
  }

  /**
   * Returns min and max (exclusive) timestamps for a specified window.
   * The function supposes that {@code duration} is the minimum batch size as it has no other information.
   * Example: with input (LocalDateTime.of(2020, 1, 1, 8, 45), "15m", 4) returns
   * (LocalDateTime.of(2020, 1, 1, 8, 0), LocalDateTime.of(2020, 1, 1, 9, 0)).
   * With input (LocalDateTime.of(2020, 1, 1, 8, 30), "1h", 3) returns
   * (LocalDateTime.of(2020, 1, 1, 6, 30), LocalDateTime.of(2020, 1, 1, 9, 30)).
   *
   * @param ts             TimeStamp that is taken as the max timestamp of the window.
   *                       As the reading works with the interval (min - inclusive, max - exclusive),
   *                       the {@code duration} is added to create the max timestamp.
   * @param duration       Duration as string, e.g. "15m" or "1h".
   * @param partitionCount The multiplier of the duration.
   * @return A tuple with min and max timestamps of the window.
   * @throws WrongArgumentException when {@code partitionCount} is not >1 as the function doesn't make sense then.
   */
  def getRangeFromWindow(ts: LocalDateTime, duration: String, partitionCount: Int): (LocalDateTime, LocalDateTime) = {
    if (partitionCount <= 1) {
      throw new WrongArgumentException("partitionCount must be bigger than 1")
    }
    val durationObj = convertDurationToObj(duration)
    val multipliedDuration = durationObj.multipliedBy(partitionCount - 1)

    (ts.minusMinutes(multipliedDuration.toMinutes), ts.plusMinutes(durationObj.toMinutes))
  }

  // Go from a string representation of a duration to a duration object
  def convertDurationToObj(duration: String): Duration = {
    duration.toCharArray.last.toLower match {
      case 'm' =>
        Duration.ofMinutes(duration.substring(0, duration.length() - 1).toInt)
      case 'h' =>
        Duration.ofHours(duration.substring(0, duration.length() - 1).toInt)
      case 'd' =>
        Duration.ofDays(duration.substring(0, duration.length() - 1).toInt)
      case _ => throw new RuntimeException("Unknown duration format")
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
   *
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
      yield {
        batch.plus(durationAdd)
      }
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
   * @param date     DateTime
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

  /**
   * Check if there are gaps in a list of LocalDateTime or if they are all in order and increasing
   *
   * @param in            A sequence for LocalDateTime
   * @param partitionSize Duration (step-size between 2 localdatetimes)
   * @return True for no-gaps
   */
  def checkIsSequentialIncreasing(in: Seq[LocalDateTime], partitionSize: Duration): Boolean = {
    if (in.length == 1) {
      return true
    }
    if (partitionSize.isNegative) {
      return false
    }
    in.sliding(2).map(pairSeq => Duration.between(pairSeq.head, pairSeq.tail.head)).forall(dur => dur == partitionSize)
  }
}
