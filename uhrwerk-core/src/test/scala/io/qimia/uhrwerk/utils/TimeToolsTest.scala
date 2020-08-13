package io.qimia.uhrwerk.utils

import java.time.{Duration, LocalDateTime, LocalTime, Month}

import com.mysql.cj.exceptions.WrongArgumentException
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
    var convertedInput = TimeTools.convertDurationStrToObj(input)
    var predictedResult = Duration.ofMinutes(45)
    assert(convertedInput.compareTo(predictedResult) == 0)
    var convertBack = TimeTools.convertDurationToStr(convertedInput)
    assert(input.compareTo(convertBack) == 0)

    input = "1h"
    convertedInput = TimeTools.convertDurationStrToObj(input)
    predictedResult = Duration.ofHours(1)
    assert(convertedInput.compareTo(predictedResult) == 0)
    convertBack = TimeTools.convertDurationToStr(convertedInput)
    assert(input.compareTo(convertBack) == 0)

    input = "3d"
    convertedInput = TimeTools.convertDurationStrToObj(input)
    predictedResult = Duration.ofDays(3)
    assert(convertedInput.compareTo(predictedResult) == 0)
    convertBack = TimeTools.convertDurationToStr(convertedInput)
    assert(input.compareTo(convertBack) == 0)
  }

  "empty duration" should "not screw up the system" in {
    val emptyDur = Duration.ZERO
    val someOut = TimeTools.convertDurationToStr(emptyDur)
    val someOut2 = TimeTools.convertDurationStrToObj(someOut)
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
    val courseDuration: Duration = Duration.ofHours(1)
    val courseListA: List[LocalDateTime] = TimeTools.convertRangeToBatch(
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

  "Creating a windowed batch list and reversing it" should "return the original batchlist and have the right size" in {
    val dateTimes = List(
      LocalDateTime.of(2010, Month.MARCH, 5, 10, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 11, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 12, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 13, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 14, 0, 0)
    )
    val windowedVersion =
      TimeTools.convertToWindowBatchList(dateTimes, Duration.ofHours(1), 4)
    assert(windowedVersion.length == (dateTimes.length + 3))
    assert(
      windowedVersion.head === LocalDateTime.of(2010, Month.MARCH, 5, 7, 0, 0))
    val outDateTimes = TimeTools.cleanWindowBatchList(windowedVersion, 4)
    assert(dateTimes === outDateTimes)
  }

  "converting a time into a batch number" should "work for common minute values" in {
    val tenMinuteTest = List(
      LocalTime.of(11, 0),
      LocalTime.of(13, 10),
      LocalTime.of(16, 20),
      LocalTime.of(3, 30),
      LocalTime.of(6, 40),
      LocalTime.of(1, 50)
    )
    val tenMinuteCorrect = List(0, 1, 2, 3, 4, 5)
    val tenMinuteResult = tenMinuteTest.map(time =>
      TimeTools.getBatchInHourNumber(time, Duration.ofMinutes(10)))
    assert(tenMinuteResult === tenMinuteCorrect)

    val quarterTest = List(
      LocalTime.of(13, 0),
      LocalTime.of(22, 15),
      LocalTime.of(3, 30),
      LocalTime.of(6, 45),
    )
    val quarterCorrect = List(0, 1, 2, 3)
    val quarterMinuteResult = quarterTest.map(time =>
      TimeTools.getBatchInHourNumber(time, Duration.ofMinutes(15)))
    assert(quarterMinuteResult === quarterCorrect)

    val halfTest = List(
      LocalTime.of(13, 0),
      LocalTime.of(22, 30),
    )
    val halfCorrect = List(0, 1)
    val halfResult = halfTest.map(time =>
      TimeTools.getBatchInHourNumber(time, Duration.ofMinutes(30)))
    assert(halfResult === halfCorrect)
  }

  "checking if one duration is divisible by another" should "return true if possible and false if not" in {
    val b1 = Duration.ofHours(6)
    val s1 = Duration.ofHours(1)
    assert(TimeTools.divisibleBy(b1, s1))
    val s11 = Duration.ofMinutes(10)
    assert(TimeTools.divisibleBy(b1, s11))
    val s12 = Duration.ofMinutes(40)
    assert(TimeTools.divisibleBy(b1, s12))
    assert(!TimeTools.divisibleBy(s1, b1))
  }

  "when converting a LocalDateTime to a postfix it" should "be easily convertable" in {
    val dateTime = LocalDateTime.of(2020, 1, 1, 8, 45)
    val duration = Duration.ofMinutes(15)
    val outStr = TimeTools.dateTimeToPostFix(dateTime, duration)
    val corrStr = s"2020-01-01-08-3"
    assert(outStr === corrStr)
  }

  "when converting a LocalDateTime to a date it" should "be easily convertable" in {
    val dateTime = LocalDateTime.of(2020, 1, 1, 8, 45)
    val outStr = TimeTools.dateTimeToDate(dateTime)
    val corrStr = s"2020-01-01"
    assert(outStr === corrStr)
  }

  "getRangeFromAggregate" should "convert an agreggate to a range" in {
    val dateTime = LocalDateTime.of(2020, 1, 1, 8, 45, 23)

    val (start, end) = TimeTools.getRangeFromAggregate(dateTime, "15m", 4)
    val (startShouldBe, endShouldBe) = (LocalDateTime.of(2020, 1, 1, 8, 0),
      LocalDateTime.of(2020, 1, 1, 9, 0))

    assert(start === startShouldBe)
    assert(end === endShouldBe)

    val (start2, end2) = TimeTools.getRangeFromAggregate(dateTime, "1h", 1)

    assert(start2 === startShouldBe)
    assert(end2 === endShouldBe)

    val (startDay, endDay) = TimeTools.getRangeFromAggregate(dateTime, "1h", 24)
    val (startDayShouldBe, endDayShouldBe) = (LocalDateTime.of(2020, 1, 1, 0, 0),
      LocalDateTime.of(2020, 1, 2, 0, 0))

    assert(startDay === startDayShouldBe)
    assert(endDay === endDayShouldBe)

    val (startDay2, endDay2) = TimeTools.getRangeFromAggregate(dateTime, "1d", 1)

    assert(startDay2 === startDayShouldBe)
    assert(endDay2 === endDayShouldBe)

    val (startMonthShouldBe, endMonthShouldBe) = (LocalDateTime.of(2020, 1, 1, 0, 0),
      LocalDateTime.of(2020, 2, 1, 0, 0))

    val (startMonth, endMonth) = TimeTools.getRangeFromAggregate(dateTime, "1d", 31)

    assert(startMonth === startMonthShouldBe)
    assert(endMonth === endMonthShouldBe)

    assertThrows[UnsupportedOperationException](TimeTools.getRangeFromAggregate(dateTime, "15m", 3))
    assertThrows[UnsupportedOperationException](TimeTools.getRangeFromAggregate(dateTime, "15m", 5))
    assertThrows[UnsupportedOperationException](TimeTools.getRangeFromAggregate(dateTime, "30m", 3))
    assertThrows[UnsupportedOperationException](TimeTools.getRangeFromAggregate(dateTime, "1h", 3))
    assertThrows[UnsupportedOperationException](TimeTools.getRangeFromAggregate(dateTime, "1d", 2))
    assertThrows[UnsupportedOperationException](TimeTools.getRangeFromAggregate(dateTime, "1h", 25))
  }

  "getRangeFromWindow" should "convert a window to a range" in {
    val dateTime = LocalDateTime.of(2020, 1, 1, 8, 45, 0)

    val (start, end) = TimeTools.getRangeFromWindow(dateTime, "15m", 4)
    val (startShouldBe, endShouldBe) = (LocalDateTime.of(2020, 1, 1, 8, 0),
      LocalDateTime.of(2020, 1, 1, 9, 0))

    assert(start === startShouldBe)
    assert(end === endShouldBe)

    val (startHour, endHour) = TimeTools.getRangeFromWindow(dateTime, "1h", 3)
    val (startHourShouldBe, endHourShouldBe) = (LocalDateTime.of(2020, 1, 1, 6, 45),
      LocalDateTime.of(2020, 1, 1, 9, 45))

    assert(startHour === startHourShouldBe)
    assert(endHour === endHourShouldBe)

    val (startDay, endDay) = TimeTools.getRangeFromWindow(dateTime, "1h", 24)
    val (startDayShouldBe, endDayShouldBe) = (LocalDateTime.of(2019, 12, 31, 9, 45),
      LocalDateTime.of(2020, 1, 1, 9, 45))

    assert(startDay === startDayShouldBe)
    assert(endDay === endDayShouldBe)

    val (startMonthShouldBe, endMonthShouldBe) = (LocalDateTime.of(2019, 12, 2, 8, 45),
      LocalDateTime.of(2020, 1, 2, 8, 45))

    val (startMonth, endMonth) = TimeTools.getRangeFromWindow(dateTime, "1d", 31)

    assert(startMonth === startMonthShouldBe)
    assert(endMonth === endMonthShouldBe)

    assertThrows[WrongArgumentException](TimeTools.getRangeFromWindow(dateTime, "15m", 1))
    assertThrows[WrongArgumentException](TimeTools.getRangeFromWindow(dateTime, "15m", 1))
    assertThrows[WrongArgumentException](TimeTools.getRangeFromWindow(dateTime, "30m", 0))
    assertThrows[WrongArgumentException](TimeTools.getRangeFromWindow(dateTime, "1h", -4))
  }

  "is duration size days" should "return the correct answer" in {
    assert(!TimeTools.isDurationSizeDays(Duration.ofHours(1)))
    assert(!TimeTools.isDurationSizeDays(Duration.ofHours(7)))
    assert(!TimeTools.isDurationSizeDays(Duration.ofMinutes(5)))
    assert(TimeTools.isDurationSizeDays(Duration.ofDays(1)))
    assert(TimeTools.isDurationSizeDays(Duration.ofDays(19)))
  }

  "a continuous datetime series" should "be marked as continuous" in {
    val dateTimes1 = List(
      LocalDateTime.of(2010, Month.MARCH, 5, 10, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 11, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 12, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 13, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 14, 0, 0)
    )
    val dur1 = Duration.ofHours(1)
    val res1 = TimeTools.checkIsSequentialIncreasing(dateTimes1, dur1)
    assert(res1)

    val dateTimes2 = List(
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 0, 0),
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 20, 0),
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 40, 0),
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 11, 0, 0)
    )
    val dur2 = Duration.ofMinutes(20)
    val res2 = TimeTools.checkIsSequentialIncreasing(dateTimes2, dur2)
    assert(res2)
  }

  "a non-continuous datetime series" should "be marked as non-continous" in {
    val dateTimes1 = List(
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 0, 0),
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 15, 0), // No 30
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 45, 0),
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 11, 0, 0)
    )
    val dur1 = Duration.ofMinutes(15)
    val res1 = TimeTools.checkIsSequentialIncreasing(dateTimes1, dur1)
    assert(!res1)

    val dateTimes2 = List(
      LocalDateTime.of(2010, Month.MARCH, 5, 12, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 11, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 10, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 9, 0, 0),
    )
    val dur2 = Duration.ofHours(1)
    val res2 = TimeTools.checkIsSequentialIncreasing(dateTimes2, dur2)
    assert(!res2)
  }

  "grouping datetimes in sequential groups" should "result in correct groups" in {
    // basic test limited by maxSize
    val someSeq = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 0, 0),
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 12, 0, 0),
      Duration.ofMinutes(10)
    )
    val grouped = TimeTools.groupSequentialIncreasing(someSeq, Duration.ofMinutes(10), 3)
    assertResult(Array(3, 3, 3, 3))(grouped)

    // One with some natural gaps
    val anotherSeq = Seq(
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 0, 0),  // 1
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 30, 0),
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 45, 0), // 2
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 12, 0, 0),  // 1
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 0, 0),
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 15, 0),
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 30, 0), // 3 (max)
      LocalDateTime.of(2020, Month.FEBRUARY, 5, 10, 45, 0)  // 1
    )
    val groupedToo = TimeTools.groupSequentialIncreasing(anotherSeq, Duration.ofMinutes(15), 3)
    assertResult(Array(1,2,1,3,1))(groupedToo)
  }
}
