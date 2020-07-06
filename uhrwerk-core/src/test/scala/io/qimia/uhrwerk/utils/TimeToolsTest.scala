package io.qimia.uhrwerk.utils

import java.time.{Duration, LocalDateTime, Month}

import org.scalatest.flatspec.AnyFlatSpec

class TimeToolsTest extends AnyFlatSpec {

  "a given duration with batchsize with A two hour range" should "be split in 8 perfect quarters" in {
    val startPoint = LocalDateTime.of(2020, Month.JANUARY, 5, 10, 0, 0)
    val endPoint = LocalDateTime.of(2020, Month.JANUARY, 5, 12, 0, 0)
    val duration = Duration.ofMinutes(15)
    val out = TimeTools.convertRangeToBatch(startPoint, endPoint, duration)

    val predictList = List(
      LocalDateTime.of(2020, Month.JANUARY, 5, 10, 0, 0),
      LocalDateTime.of(2020, Month.JANUARY, 5, 10, 15, 0),
      LocalDateTime.of(2020, Month.JANUARY, 5, 10, 30, 0),
      LocalDateTime.of(2020, Month.JANUARY, 5, 10, 45, 0),
      LocalDateTime.of(2020, Month.JANUARY, 5, 11, 0, 0),
      LocalDateTime.of(2020, Month.JANUARY, 5, 11, 15, 0),
      LocalDateTime.of(2020, Month.JANUARY, 5, 11, 30, 0),
      LocalDateTime.of(2020, Month.JANUARY, 5, 11, 45, 0)
    )
    out.zip(predictList).foreach((tup) => assert(tup._1.compareTo(tup._2) == 0))
  }

  "a given duration with batchsize with A two hour range over 2 days" should "be split in 8 perfect quarters" in {
    val startPoint = LocalDateTime.of(2020, Month.JANUARY, 5, 23, 0, 0)
    val endPoint = LocalDateTime.of(2020, Month.JANUARY, 6, 1, 0, 0)
    val duration = Duration.ofMinutes(15)
    val out = TimeTools.convertRangeToBatch(startPoint, endPoint, duration)

    val predictList = List(
      LocalDateTime.of(2020, Month.JANUARY, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.JANUARY, 5, 23, 15, 0),
      LocalDateTime.of(2020, Month.JANUARY, 5, 23, 30, 0),
      LocalDateTime.of(2020, Month.JANUARY, 5, 23, 45, 0),
      LocalDateTime.of(2020, Month.JANUARY, 6, 0, 0, 0),
      LocalDateTime.of(2020, Month.JANUARY, 6, 0, 15, 0),
      LocalDateTime.of(2020, Month.JANUARY, 6, 0, 30, 0),
      LocalDateTime.of(2020, Month.JANUARY, 6, 0, 45, 0)
    )
    out.zip(predictList).foreach((tup) => assert(tup._1.compareTo(tup._2) == 0))
  }

  "a range with a too large stepsize" should "be round down to lowest number of full batches" in {
    val startPoint = LocalDateTime.of(2020, Month.JANUARY, 5, 20, 0, 0)
    val endPoint = LocalDateTime.of(2020, Month.JANUARY, 5, 20, 30, 0)
    val duration = Duration.ofHours(1)
    val out = TimeTools.convertRangeToBatch(startPoint, endPoint, duration)
    assert(out.length === 0)

    val out2 = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2020, Month.JANUARY, 5, 20, 0, 0),
      LocalDateTime.of(2020, Month.JANUARY, 5, 22, 30, 0),
      duration)
    assert(out2.length === 2)
  }

  "a duration string and matching object" should "be convertable from one to the other and back" in {
    var input = "45m"
    var convertedInput = TimeTools.convertDurationToObj(input)
    var predictedResult = Duration.ofMinutes(45)
    assert(convertedInput.compareTo(predictedResult) == 0)
    var convertBack = TimeTools.convertDurationToStr(convertedInput)
    assert(input.compareTo(convertBack) == 0)

    input = "1h"
    convertedInput = TimeTools.convertDurationToObj(input)
    predictedResult = Duration.ofHours(1)
    assert(convertedInput.compareTo(predictedResult) == 0)
    convertBack = TimeTools.convertDurationToStr(convertedInput)
    assert(input.compareTo(convertBack) == 0)

    input = "3d"
    convertedInput = TimeTools.convertDurationToObj(input)
    predictedResult = Duration.ofDays(3)
    assert(convertedInput.compareTo(predictedResult) == 0)
    convertBack = TimeTools.convertDurationToStr(convertedInput)
    assert(input.compareTo(convertBack) == 0)
  }

  "empty duration" should "not screw up the system" in {
    val emptyDur = Duration.ZERO
    val someOut = TimeTools.convertDurationToStr(emptyDur)
    val someOut2 = TimeTools.convertDurationToObj(someOut)
    assert(emptyDur === someOut2)
  }

  "a datetime string" should "be converted to a localdatetime object and viceversa" in {
    val aDate = LocalDateTime.of(2020, Month.JANUARY, 5, 23, 0, 0)
    val aDateStr = TimeTools.convertTSToString(aDate)
    val predictedStr = "2020-01-05T23:00:00"
    assert(aDateStr.compareTo(predictedStr) == 0)
    val outDate = TimeTools.convertTSToTimeObj(aDateStr)
    assert(outDate.compareTo(aDate) == 0)
  }

  "a course datetime list" should "be converted in smaller parts (even when the larger is not whole)" in {
    val courseDuration = Duration.ofHours(1)
    val courseListA = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2020, Month.JANUARY, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.JANUARY, 6, 14, 0, 0),
      courseDuration)
    val outA =
      TimeTools.convertToSmallerBatchList(courseListA, courseDuration, 4)
    val fineListA = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2020, Month.JANUARY, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.JANUARY, 6, 14, 0, 0),
      Duration.ofMinutes(15))
    outA
      .zip(fineListA)
      .foreach(timePair => assert(timePair._1.isEqual(timePair._2)))

    val courseB1 = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2010, Month.MARCH, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.MARCH, 10, 14, 0, 0),
      courseDuration)
    val courseB2 = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2010, Month.JULY, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.JULY, 10, 14, 0, 0),
      courseDuration)
    val courseB = courseB1 ::: courseB2
    val outB = TimeTools.convertToSmallerBatchList(courseB, courseDuration, 2)

    val fineB1 = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2010, Month.MARCH, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.MARCH, 10, 14, 0, 0),
      Duration.ofMinutes(30))
    val fineB2 = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2010, Month.JULY, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.JULY, 10, 14, 0, 0),
      Duration.ofMinutes(30))
    val fineListB = fineB1 ::: fineB2
    outB
      .zip(fineListB)
      .foreach(timePair => assert(timePair._1.isEqual(timePair._2)))
  }

  "A course datetime list filtered with a fine datetime list" should "only return values when the full duration is covered" in {
    val courseDuration = Duration.ofMinutes(30)
    val dividerA = 3
    val courseListA = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2020, Month.JANUARY, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.JANUARY, 6, 14, 0, 0),
      courseDuration)
    val fineListA =
      TimeTools.convertToSmallerBatchList(courseListA, courseDuration, dividerA)
    val courseResultList = TimeTools.filterBySmallerBatchList(courseListA,
      courseDuration,
      dividerA,
      fineListA.toSet)
    assert(courseListA.toSet.equals(courseResultList.toSet))

    val dividerB = 2
    val courseListB1 = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2010, Month.MARCH, 5, 10, 0, 0),
      LocalDateTime.of(2020, Month.MARCH, 5, 12, 0, 0),
      courseDuration)
    val courseListB2 = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2010, Month.MARCH, 7, 15, 0, 0),
      LocalDateTime.of(2020, Month.MARCH, 7, 17, 0, 0),
      courseDuration)
    val courseListB = courseListB1 ::: courseListB2

    val fineListB = List(
      LocalDateTime.of(2010, Month.MARCH, 5, 10, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 10, 15, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 10, 30, 0), // gap
      LocalDateTime.of(2010, Month.MARCH, 5, 11, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 11, 15, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 11, 30, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 11, 45, 0),
      LocalDateTime.of(2010, Month.MARCH, 7, 15, 0, 0), // gap gap
      LocalDateTime.of(2010, Month.MARCH, 7, 15, 45, 0),
      LocalDateTime.of(2010, Month.MARCH, 7, 16, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 7, 16, 15, 0),
      LocalDateTime.of(2010, Month.MARCH, 7, 16, 30, 0),
      LocalDateTime.of(2010, Month.MARCH, 7, 16, 45, 0)
    )

    val predictedResultB = List(
      LocalDateTime.of(2010, Month.MARCH, 5, 10, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 11, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 11, 30, 0),
      LocalDateTime.of(2010, Month.MARCH, 7, 16, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 7, 16, 30, 0)
    )
    val outB = TimeTools.filterBySmallerBatchList(courseListB,
      courseDuration,
      dividerB,
      fineListB.toSet)
    outB
      .zip(predictedResultB)
      .foreach(timePair => assert(timePair._1.isEqual(timePair._2)))
  }

}

