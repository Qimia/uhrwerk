package io.qimia.uhrwerk.stepwrapper

import java.time.{LocalDateTime, Month}

import io.qimia.uhrwerk.StepWrapper
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class StepWrapperUnitTest extends AnyFlatSpec {

  "creating a window filter" should "filter out the right values" in {
    val testA = ArrayBuffer.fill(12)(true)
    testA(3) = false
    val out = StepWrapper.createFilterWithWindowList(testA.toList, 3)
    val falseIdx = Set(3, 4, 5)
    out.zipWithIndex.map(tup => if (falseIdx.contains(tup._2)) {
      assert(!tup._1)
    } else {
      assert(tup._1)
    })

    testA(4) = false
    testA(5) = false
    val out2 = StepWrapper.createFilterWithWindowList(testA.toList, 3)
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
    val filteredList = StepWrapper.applyWindowFilter(testCheckOutput.toList, booleanFilter, "fail_path")
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

}
