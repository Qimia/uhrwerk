package io.qimia.uhrwerk.common.tools

import java.time.{Duration, LocalDateTime, Month}

import io.qimia.uhrwerk.common.model.PartitionUnit
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
    out.zip(predictList).foreach(tup => assert(tup._1.compareTo(tup._2) == 0))
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
    out.zip(predictList).foreach(tup => assert(tup._1.compareTo(tup._2) == 0))
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
      duration
    )
    assert(out2.length === 2)
  }

  "a course datetime list" should "be converted in smaller parts (even when the larger is not whole)" in {
    val courseDuration: Duration = Duration.ofHours(1)
    val courseListA: List[LocalDateTime] = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2020, Month.JANUARY, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.JANUARY, 6, 14, 0, 0),
      courseDuration
    )
    val outA =
      TimeTools.convertToSmallerBatchList(courseListA, courseDuration, 4)
    val fineListA = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2020, Month.JANUARY, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.JANUARY, 6, 14, 0, 0),
      Duration.ofMinutes(15)
    )
    outA
      .zip(fineListA)
      .foreach(timePair => assert(timePair._1.isEqual(timePair._2)))

    val courseB1 = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2010, Month.MARCH, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.MARCH, 10, 14, 0, 0),
      courseDuration
    )
    val courseB2 = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2010, Month.JULY, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.JULY, 10, 14, 0, 0),
      courseDuration
    )
    val courseB = courseB1 ::: courseB2
    val outB = TimeTools.convertToSmallerBatchList(courseB, courseDuration, 2)

    val fineB1 = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2010, Month.MARCH, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.MARCH, 10, 14, 0, 0),
      Duration.ofMinutes(30)
    )
    val fineB2 = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2010, Month.JULY, 5, 23, 0, 0),
      LocalDateTime.of(2020, Month.JULY, 10, 14, 0, 0),
      Duration.ofMinutes(30)
    )
    val fineListB = fineB1 ::: fineB2
    outB
      .zip(fineListB)
      .foreach(timePair => assert(timePair._1.isEqual(timePair._2)))
  }

  "getAggregateForTimestamp with days" should "properly assign a timestamp into its aggregate" in {
    val partitionTS = Array(
      LocalDateTime.of(2012, 5, 1, 0, 0),
      LocalDateTime.of(2012, 5, 4, 0, 0)
    )
    val partitionUnit = PartitionUnit.DAYS.toString
    val partitionSize = 3

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2012, 5, 1, 0, 0),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2012, 5, 1, 0, 0)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2012, 5, 2, 0, 0),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2012, 5, 1, 0, 0)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2012, 5, 3, 0, 0),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2012, 5, 1, 0, 0)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2012, 5, 4, 0, 0),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2012, 5, 4, 0, 0)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2012, 5, 5, 0, 0),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2012, 5, 4, 0, 0)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2012, 5, 6, 0, 0),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2012, 5, 4, 0, 0)
    )
  }

  "getAggregateForTimestamp with minutes" should "properly assign a timestamp into its aggregate" in {
    val partitionTS = Array(
      LocalDateTime.of(2020, 9, 7, 15, 0),
      LocalDateTime.of(2020, 9, 7, 15, 45),
      LocalDateTime.of(2020, 9, 7, 16, 30)
    )
    val partitionUnit = PartitionUnit.MINUTES.toString
    val partitionSize = 45

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2020, 9, 7, 15, 0),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2020, 9, 7, 15, 0)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2020, 9, 7, 15, 15),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2020, 9, 7, 15, 0)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2020, 9, 7, 15, 30),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2020, 9, 7, 15, 0)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2020, 9, 7, 15, 45),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2020, 9, 7, 15, 45)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2020, 9, 7, 16, 0),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2020, 9, 7, 15, 45)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2020, 9, 7, 16, 15),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2020, 9, 7, 15, 45)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2020, 9, 7, 16, 30),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2020, 9, 7, 16, 30)
    )

    assert(
      TimeTools.getAggregateForTimestamp(
        partitionTS,
        LocalDateTime.of(2020, 9, 7, 16, 45),
        partitionUnit,
        partitionSize
      ) === LocalDateTime.of(2020, 9, 7, 16, 30)
    )
  }

  "getAggregateForTimestamp with weeks" should "properly assign a timestamp into its aggregate" in {
    val partitionTS = Array(
      LocalDateTime.of(2020, 9, 1, 0, 0),
      LocalDateTime.of(2020, 9, 8, 0, 0),
      LocalDateTime.of(2020, 9, 15, 0, 0)
    )
    val partitionUnit = PartitionUnit.WEEKS.toString
    val partitionSize = 1

    Range(1, 8).foreach(day => {
      assert(
        TimeTools.getAggregateForTimestamp(
          partitionTS,
          LocalDateTime.of(2020, 9, day, 0, 0),
          partitionUnit,
          partitionSize
        ) === LocalDateTime.of(2020, 9, 1, 0, 0)
      )
    })

    Range(8, 15).foreach(day => {
      assert(
        TimeTools.getAggregateForTimestamp(
          partitionTS,
          LocalDateTime.of(2020, 9, day, 0, 0),
          partitionUnit,
          partitionSize
        ) === LocalDateTime.of(2020, 9, 8, 0, 0)
      )
    })

    Range(15, 22).foreach(day => {
      assert(
        TimeTools.getAggregateForTimestamp(
          partitionTS,
          LocalDateTime.of(2020, 9, day, 0, 0),
          partitionUnit,
          partitionSize
        ) === LocalDateTime.of(2020, 9, 15, 0, 0)
      )
    })
  }

  "lowerBoundOfDate" should "get the lower bound based on a partition timestamp, a current timestamp, a partition unit and size" in {
    val partitionTs = LocalDateTime.of(2020, 8, 26, 14, 30)
    val now = LocalDateTime.of(2020, 9, 22, 16, 29)
    val partitionUnitWeeks = PartitionUnit.WEEKS
    val partitionSize1 = 1

    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitWeeks, partitionSize1) === LocalDateTime.of(2020, 9, 16, 14, 30))

    val partitionSize2 = 2

    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitWeeks, partitionSize2) === LocalDateTime.of(2020, 9, 9, 14, 30))

    val partitionSize4 = 4

    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitWeeks, partitionSize4) === LocalDateTime.of(2020, 8, 26, 14, 30))

    val partitionUnitDays = PartitionUnit.DAYS

    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitDays, partitionSize1) === LocalDateTime.of(2020, 9, 22, 14, 30))
    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitDays, partitionSize2) === LocalDateTime.of(2020, 9, 21, 14, 30))
    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitDays, partitionSize4) === LocalDateTime.of(2020, 9, 19, 14, 30))

    val partitionUnitHours = PartitionUnit.HOURS

    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitHours, partitionSize1) === LocalDateTime.of(2020, 9, 22, 15, 30))
    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitHours, partitionSize2) === LocalDateTime.of(2020, 9, 22, 14, 30))
    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitHours, partitionSize4) === LocalDateTime.of(2020, 9, 22, 14, 30))

    val partitionUnitMinutes = PartitionUnit.MINUTES

    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitMinutes, partitionSize1) === LocalDateTime.of(2020, 9, 22, 16, 29))
    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitMinutes, partitionSize2) === LocalDateTime.of(2020, 9, 22, 16, 28))
    assert(TimeTools.lowerBoundOfCurrentTimestamp(partitionTs, now, partitionUnitMinutes, partitionSize4) === LocalDateTime.of(2020, 9, 22, 16, 26))
  }
}
