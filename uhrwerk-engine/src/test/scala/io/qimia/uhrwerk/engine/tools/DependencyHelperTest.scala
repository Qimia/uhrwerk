package io.qimia.uhrwerk.engine.tools

import io.qimia.uhrwerk.common.metastore.builders.ConnectionModelBuilder
import io.qimia.uhrwerk.common.metastore.dependency.{DependencyResult, TablePartitionResult, TablePartitionResultSet}
import io.qimia.uhrwerk.common.metastore.model.{DependencyModel, Partition, PartitionTransformType, PartitionUnit}
import io.qimia.uhrwerk.common.model._
import org.scalatest.flatspec.AnyFlatSpec

import java.time.{Duration, LocalDateTime}
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
      LocalDateTime.of(2010, 4, 5, 13, 0)
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

    val outRes = DependencyHelper.createTablePartitionResultGroups(
      totalResult,
      duration,
      3
    )

    @tailrec
    def checkElement(
        outRes: List[Array[TablePartitionResult]],
        correctAns: List[Array[LocalDateTime]]
    ): Unit = {
      val groupedOut = outRes.head
      val groupedAns = correctAns.head
      groupedAns
        .zip(groupedOut)
        .foreach(ansResPair =>
          assert(ansResPair._1 === ansResPair._2.getPartitionTs)
        )
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
        LocalDateTime.of(2010, 4, 5, 12, 0)
      ),
      Array(
        LocalDateTime.of(2010, 4, 5, 11, 0),
        LocalDateTime.of(2010, 4, 5, 12, 0),
        LocalDateTime.of(2010, 4, 5, 13, 0)
      ),
      Array(
        LocalDateTime.of(2010, 4, 5, 12, 0),
        LocalDateTime.of(2010, 4, 5, 13, 0),
        LocalDateTime.of(2010, 4, 5, 14, 0)
      )
    )
    val partitions = times.map(timeGroup =>
      timeGroup.map(t => {
        val newPart = new Partition()
        newPart.setTargetId(targetId)
        newPart.setPartitionSize(1)
        newPart.setPartitionUnit(PartitionUnit.HOURS)
        newPart.setPartitionTs(t)
        newPart
      })
    )
    val outRes = partitions.flatten.distinct
    val expectedRes = Array(
      partitions(0)(0),
      partitions(0)(1),
      partitions(0)(2),
      partitions(1)(2),
      partitions(2)(2)
    )
    assert(outRes === expectedRes)
  }

  "Going from TablePartitionResult to BulkDependencyResult" should "result in the same data on a different axis" in {
    val conn1 = new ConnectionModelBuilder()
      .name("testConn")
      .build()

    val depA = new DependencyModel()
    depA.setTableName("sometable")
    depA.setTransformType(PartitionTransformType.IDENTITY)
    depA.setTransformPartitionSize(1)
    val depB = new DependencyModel()
    depB.setTableName("anothertable")
    depB.setTransformType(PartitionTransformType.AGGREGATE)
    depB.setTransformPartitionSize(2)

    val time1 = LocalDateTime.of(2010, 4, 5, 10, 0)
    val res1 = new TablePartitionResult()
    res1.setResolved(true)
    res1.setProcessed(false)
    res1.setPartitionTs(time1)
    val res1depA = new DependencyResult()
    res1depA.setSuccess(true)
    res1depA.setConnection(conn1)
    res1depA.setDependency(depA)
    res1depA.setPartitionTs(time1)
    res1depA.setSucceeded(Array(time1))
    val res1depAPart1 = new Partition()
    res1depAPart1.setPartitionUnit(PartitionUnit.HOURS)
    res1depAPart1.setPartitionSize(1)
    res1depAPart1.setPartitionTs(time1)
    res1depA.setPartitions(Array(res1depAPart1))
    val res1depB = new DependencyResult()
    res1depB.setSuccess(true)
    res1depB.setConnection(conn1)
    res1depB.setDependency(depB)
    res1depB.setPartitionTs(time1)
    val time1Xtra = LocalDateTime.of(2010, 4, 5, 10, 30)
    res1depB.setSucceeded(Array(time1, time1Xtra))
    val res1depBPart1 = new Partition()
    res1depBPart1.setPartitionUnit(PartitionUnit.MINUTES)
    res1depBPart1.setPartitionSize(30)
    res1depBPart1.setPartitionTs(time1)
    val res1depBPart2 = new Partition()
    res1depBPart2.setPartitionUnit(PartitionUnit.MINUTES)
    res1depBPart2.setPartitionSize(30)
    res1depBPart2.setPartitionTs(time1Xtra)
    res1depB.setPartitions(Array(res1depBPart1, res1depBPart2))
    res1.setResolvedDependencies(Array(res1depA, res1depB))

    val time2 = LocalDateTime.of(2010, 4, 5, 11, 0)
    val res2 = new TablePartitionResult()
    res2.setResolved(true)
    res2.setProcessed(false)
    res2.setPartitionTs(time2)
    val res2depA = new DependencyResult()
    res2depA.setSuccess(true)
    res2depA.setConnection(conn1)
    res2depA.setDependency(depA)
    res2depA.setPartitionTs(time2)
    res2depA.setSucceeded(Array(time2))
    val res2depAPart1 = new Partition()
    res2depAPart1.setPartitionUnit(PartitionUnit.HOURS)
    res2depAPart1.setPartitionSize(1)
    res2depAPart1.setPartitionTs(time2)
    res2depA.setPartitions(Array(res2depAPart1))
    val res2depB = new DependencyResult()
    res2depB.setSuccess(true)
    res2depB.setConnection(conn1)
    res2depB.setDependency(depB)
    res2depB.setPartitionTs(time2)
    val time2Xtra = LocalDateTime.of(2010, 4, 5, 11, 30)
    res2depB.setSucceeded(Array(time2, time2Xtra))
    val res2depBPart1 = new Partition()
    res2depBPart1.setPartitionUnit(PartitionUnit.MINUTES)
    res2depBPart1.setPartitionSize(30)
    res2depBPart1.setPartitionTs(time2)
    val res2depBPart2 = new Partition()
    res2depBPart2.setPartitionUnit(PartitionUnit.MINUTES)
    res2depBPart2.setPartitionSize(30)
    res2depBPart2.setPartitionTs(time2Xtra)
    res2depB.setPartitions(Array(res2depBPart1, res2depBPart2))
    res2.setResolvedDependencies(Array(res2depA, res2depB))

    val time3 = LocalDateTime.of(2010, 4, 5, 12, 0)
    val res3 = new TablePartitionResult()
    res3.setResolved(true)
    res3.setProcessed(false)
    res3.setPartitionTs(time3)
    val res3depA = new DependencyResult()
    res3depA.setSuccess(true)
    res3depA.setConnection(conn1)
    res3depA.setDependency(depA)
    res3depA.setPartitionTs(time3)
    res3depA.setSucceeded(Array(time3))
    val res3depAPart1 = new Partition()
    res3depAPart1.setPartitionUnit(PartitionUnit.HOURS)
    res3depAPart1.setPartitionSize(1)
    res3depAPart1.setPartitionTs(time3)
    res3depA.setPartitions(Array(res3depAPart1))
    val res3depB = new DependencyResult()
    res3depB.setSuccess(true)
    res3depB.setConnection(conn1)
    res3depB.setDependency(depB)
    res3depB.setPartitionTs(time3)
    val time3Xtra = LocalDateTime.of(2010, 4, 5, 12, 30)
    res3depB.setSucceeded(Array(time3, time3Xtra))
    val res3depBPart1 = new Partition()
    res3depBPart1.setPartitionUnit(PartitionUnit.MINUTES)
    res3depBPart1.setPartitionSize(30)
    res3depBPart1.setPartitionTs(time3)
    val res3depBPart2 = new Partition()
    res3depBPart2.setPartitionUnit(PartitionUnit.MINUTES)
    res3depBPart2.setPartitionSize(30)
    res3depBPart2.setPartitionTs(time3Xtra)
    res3depB.setPartitions(Array(res3depBPart1, res3depBPart2))
    res3.setResolvedDependencies(Array(res3depA, res3depB))

    // Now lets pivot this
    val testRes =
      DependencyHelper.extractBulkDependencyResult(Array(res1, res2, res3))
    assert(2 === testRes.length)
    val depABulk = testRes.head
    assert(depABulk.dependency.getTableName === "sometable")
    assert(depABulk.partitionTimestamps === Array(time1, time2, time3))
    val depBBulk = testRes(1)
    assert(
      depBBulk.succeeded.map(_.getPartitionTs) === Array(
        time1,
        time1Xtra,
        time2,
        time2Xtra,
        time3,
        time3Xtra
      )
    )
  }

}
