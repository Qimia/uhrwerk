package io.qimia.uhrwerk.stepwrapper

import java.time.{LocalDateTime, Month}

import io.qimia.uhrwerk.TableWrapper
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class TableWrapperUnitTest extends AnyFlatSpec {

  "creating a window filter" should "filter out the right values" in {
    val testA = ArrayBuffer.fill(12)(true)
    testA(3) = false
    val out = TableWrapper.createFilterWithWindowList(testA.toList, 3)
    val falseIdx = Set(3, 4, 5)
    out.zipWithIndex.map(tup => if (falseIdx.contains(tup._2)) {
      assert(!tup._1)
    } else {
      assert(tup._1)
    })

    testA(4) = false
    testA(5) = false
    val out2 = TableWrapper.createFilterWithWindowList(testA.toList, 3)
    val falseIdx2 = Set(3, 4, 5, 6, 7)
    out2.zipWithIndex.map(tup => if (falseIdx2.contains(tup._2)) {
      assert(!tup._1)
    } else {
      assert(tup._1)
    })
  }

  "applying a boolean filter to a dependency-check" should "return the correct dependency-check-outcome" in {
    val dates = Array(
      LocalDateTime.of(2010, Month.MARCH, 5, 5, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 6, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 7, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 8, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 9, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 10, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 11, 0, 0)
    )
    val testCheckOutput = Array(
      Right(dates(0)),
      Right(dates(1)),
      Left((dates(2), Set("fail_path"))),
      Right(dates(3)),
      Right(dates(4)),
      Right(dates(5)),
      Right(dates(6))
    )
    val booleanFilter = List(
      false,
      true,
      false,
      true,
      true,
      false,
      false
    )
    val filteredList = TableWrapper.applyWindowFilter(testCheckOutput.toList, booleanFilter, "fail_path")
    val leftIndices = Set(0, 2, 5, 6)
    filteredList.zipWithIndex.foreach(tup => {
      if(leftIndices.contains(tup._2)) {
        assert(tup._1.isLeft)
      } else {
        assert(tup._1.isRight)
      }
      tup._1 match {
        case Right(d) => assert(d === dates(tup._2))
        case Left(dsTup) => {
          assert(dsTup._1 === dates(tup._2))
          assert(dsTup._2 === Set("fail_path"))
        }
      }
    })
  }

  "combining multiple dependency checks" should "merge all failures and successes" in {
    val dates = Array(
      LocalDateTime.of(2010, Month.MARCH, 5, 5, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 6, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 7, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 8, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 9, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 10, 0, 0),
      LocalDateTime.of(2010, Month.MARCH, 5, 11, 0, 0)
    )
    val checkOut1 = List(
      Left(dates(0), Set("table_a")),
      Right(dates(1)),
      Left((dates(2), Set("table_b"))),
      Right(dates(3)),
      Right(dates(4)),
      Left(dates(5), Set("table_a", "table_b")),
      Right(dates(6))
    )
    val checkOut2 = List(
      Right(dates(0)),
      Right(dates(1)),
      Left(dates(2), Set("table_c", "table_d")),
      Left(dates(3), Set("table_c")),
      Right(dates(4)),
      Left(dates(5), Set("table_a", "table_c")),
      Left(dates(6), Set("table_c", "table_e"))
    )
    val mergeChecks = TableWrapper.combineDependencyChecks(checkOut1, checkOut2)
    val correctMerge = List(
      Left(dates(0), Set("table_a")),
      Right(dates(1)),
      Left(dates(2), Set("table_b", "table_c", "table_d")),
      Left(dates(3), Set("table_c")),
      Right(dates(4)),
      Left(dates(5), Set("table_a", "table_b", "table_c")),
      Left(dates(6), Set("table_c", "table_e"))
    )
    assert(mergeChecks === correctMerge)
  }

}
