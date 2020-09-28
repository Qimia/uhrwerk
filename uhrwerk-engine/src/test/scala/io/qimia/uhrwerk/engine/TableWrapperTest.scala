package io.qimia.uhrwerk.engine

import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.common.model.{Partition, PartitionUnit, Table}
import org.scalatest.flatspec.AnyFlatSpec

class TableWrapperTest extends AnyFlatSpec {
  "createPartitions" should "correctly generate Parition objects" in {
    val ts = Array(LocalDateTime.of(2020, 9, 14, 15, 0), LocalDateTime.of(2020, 9, 14, 15, 30))
    val partitionUnit = PartitionUnit.MINUTES
    val partitionSize = 30
    val targetId = 1234L

    val partitions = TableWrapper.createPartitions(ts, partitioned = true, partitionUnit, partitionSize, targetId)

    assert(partitions.length == 2)
    partitions
      .zip(ts)
      .foreach((pTs: (Partition, LocalDateTime)) => {
        val p = pTs._1
        val timestamp = pTs._2

        assert(p.isPartitioned)
        assert(p.getPartitionSize == partitionSize)
        assert(p.getPartitionTs == timestamp)
        assert(p.getPartitionUnit == partitionUnit)
        assert(p.getTargetId == targetId)
      })
  }

  "tableDuration" should "get calculated properly when initiating TableWrapper" in {
    val partitionedTable = new Table
    partitionedTable.setPartitioned(true)
    partitionedTable.setPartitionUnit(PartitionUnit.DAYS)
    partitionedTable.setPartitionSize(1)

    assert(new TableWrapper(null, partitionedTable, null, null).tableDuration == Duration.ofDays(1))

    partitionedTable.setPartitionUnit(PartitionUnit.MINUTES)
    partitionedTable.setPartitionSize(45)

    assert(new TableWrapper(null, partitionedTable, null, null).tableDuration == Duration.ofMinutes(45))

    val unpartitionedTable = new Table
    unpartitionedTable.setPartitioned(false)

    assert(new TableWrapper(null, unpartitionedTable, null, null).tableDuration == Duration.ofMinutes(1))
  }
}
