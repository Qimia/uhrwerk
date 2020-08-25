package io.qimia.uhrwerk.engine.tools

import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.common.metastore.dependency.{TablePartitionResult, TablePartitionResultSet}
import io.qimia.uhrwerk.common.model.{Partition, PartitionUnit}
import org.scalatest.flatspec.AnyFlatSpec

import scala.annotation.tailrec

class DependencyHelperTest extends AnyFlatSpec {

  "grouping TablePartitionResults" should "result in groups of continuous datetime ranges" in {
    val duration = Duration.ofMinutes(20)
    val times = Array(
      LocalDateTime.of(2010, 4, 5, 10, 0),
      LocalDateTime.of(2010, 4, 5, 10, 20),
      LocalDateTime.of(2010, 4, 5, 10, 40),
      LocalDateTime.of(2010, 4, 5, 11, 0),
      LocalDateTime.of(2010, 4, 5, 12, 0),
      LocalDateTime.of(2010, 4, 5, 12, 40),
      LocalDateTime.of(2010, 4, 5, 13, 0),
    )
    val singlePartResults: Array[TablePartitionResult] = times.map(t => {
      val tpr = new TablePartitionResult()
      tpr.setPartitionTs(t)
      tpr.setProcessed(false)
      tpr.setResolved(true)
      tpr.setResolvedDependencies(Array())
      tpr
    })

    val totalResult = new TablePartitionResultSet()
    totalResult.setResolvedTs(times)
    totalResult.setResolved(singlePartResults)

    val outRes = DependencyHelper.createTablePartitionResultGroups(totalResult, duration, 3)

    @tailrec
    def checkElement(outRes: List[Array[TablePartitionResult]], correctAns: List[Array[LocalDateTime]]): Unit = {
      val groupedOut = outRes.head
      val groupedAns = correctAns.head
      groupedAns.zip(groupedOut).foreach(ansResPair => assert(ansResPair._1 === ansResPair._2.getPartitionTs))
      if (outRes.tail.nonEmpty) {
        checkElement(outRes.tail, correctAns.tail)
      }
    }

    val correctAnswers = List(
      Array(
        LocalDateTime.of(2010, 4, 5, 10, 0),
        LocalDateTime.of(2010, 4, 5, 10, 20),
        LocalDateTime.of(2010, 4, 5, 10, 40)
      ),
      Array(
        LocalDateTime.of(2010, 4, 5, 11, 0)
      ),
      Array(
        LocalDateTime.of(2010, 4, 5, 12, 0)
      ),
      Array(
        LocalDateTime.of(2010, 4, 5, 12, 40),
        LocalDateTime.of(2010, 4, 5, 13, 0)
      )
    )

    checkElement(outRes, correctAnswers)
  }

  "flattening and combining Partitions" should "result in a single list of partitions" in {
    val targetId = 123L
    val times = Array(
      Array(
        LocalDateTime.of(2010, 4, 5, 10, 0),
        LocalDateTime.of(2010, 4, 5, 11, 0),
        LocalDateTime.of(2010, 4, 5, 12, 0),
      ),
      Array(
        LocalDateTime.of(2010, 4, 5, 11, 0),
        LocalDateTime.of(2010, 4, 5, 12, 0),
        LocalDateTime.of(2010, 4, 5, 13, 0),
      ),
      Array(
        LocalDateTime.of(2010, 4, 5, 12, 0),
        LocalDateTime.of(2010, 4, 5, 13, 0),
        LocalDateTime.of(2010, 4, 5, 14, 0),
      )
    )
    val partitions = times.map(timeGroup => timeGroup.map(t => {
      val newPart = new Partition()
      newPart.setTargetId(targetId)
      newPart.setPartitionSize(1)
      newPart.setPartitionUnit(PartitionUnit.HOURS)
      newPart.setPartitionTs(t)
      newPart.setKey()
      newPart
    }))
    val outRes = partitions.flatten.distinct
    val expectedRes = Array(
      partitions(0)(0),
      partitions(0)(1),
      partitions(0)(2),
      partitions(1)(2),
      partitions(2)(2),
    )
    assert(outRes === expectedRes)
  }

}
