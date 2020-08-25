package io.qimia.uhrwerk.engine.tools

import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.common.metastore.dependency.{DependencyResult, TablePartitionResult, TablePartitionResultSet}
import io.qimia.uhrwerk.common.model.{Connection, Dependency, Partition, PartitionTransformType, PartitionUnit}
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

  "Going from TablePartitionResult to BulkDependencyResult" should "result in the same data on a different axis" in {
    val conn1 = new Connection()
    conn1.setName("testConn")
    val depA = new Dependency()
    depA.setTableName("sometable")
    depA.setTransformType(PartitionTransformType.IDENTITY)
    depA.setTransformPartitionSize(1)
    val depB = new Dependency()
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
    res1depB.setSucceeded(Array(time1, LocalDateTime.of(2010, 4, 5, 10, 30)))
    val res1depBPart1 = new Partition()
    res1depBPart1.setPartitionUnit(PartitionUnit.MINUTES)
    res1depBPart1.setPartitionSize(30)
    res1depBPart1.setPartitionTs(time1)
    val res1depBPart2 = new Partition()
    res1depBPart2.setPartitionUnit(PartitionUnit.MINUTES)
    res1depBPart2.setPartitionSize(30)
    res1depBPart2.setPartitionTs(LocalDateTime.of(2010, 4, 5, 10, 30))
    res1depA.setPartitions(Array(res1depBPart1, res1depBPart2))
    res1.setResolvedDependencies(Array(res1depA, res1depB))

    // TODO add 2 more TablePartitionResult and then use `extractBulkDependencyResult`
  }

}
