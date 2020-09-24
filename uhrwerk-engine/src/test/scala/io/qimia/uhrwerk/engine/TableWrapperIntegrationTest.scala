package io.qimia.uhrwerk.engine

import java.sql.DriverManager
import java.time.LocalDateTime

import io.qimia.uhrwerk.common.model.{Partition, PartitionUnit, Table, Metastore => MetastoreConnInfo}
import io.qimia.uhrwerk.config.{ConnectionBuilder, TableBuilder}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

class TableWrapperIntegrationTest extends AnyFlatSpec with BeforeAndAfterEach {

  val metastoreUrl = "jdbc:mysql://localhost:53306/UHRWERK_METASTORE_UNIT_TESTS"

  val testConnInfo = new MetastoreConnInfo
  testConnInfo.setJdbc_url(metastoreUrl)
  testConnInfo.setJdbc_driver("com.mysql.cj.jdbc.Driver")
  testConnInfo.setUser("UHRWERK_USER")
  testConnInfo.setPass("Xq92vFqEKF7TB8H9")
  val metaStore = MetaStore.build(testConnInfo)

  var table: Table = _

  override protected def beforeEach(): Unit = {
    try super.beforeEach()
    val connBuilder1               = new ConnectionBuilder()
    val singleSourceTestConnection = connBuilder1.name("testConn").file.path("/some/path/on/disk/").done.build
    metaStore.connectionService.save(singleSourceTestConnection, false)
    val connBuilder2             = new ConnectionBuilder()
    val singleDataLakeConnection = connBuilder2.name("testDataLake").file.path("/other/path/on/disk/").done.build
    metaStore.connectionService.save(singleDataLakeConnection, false)

    // Setup some table (taken from ProcessPartitionIntegrationTest
    val tableBuilderA = new TableBuilder
    val tableA = tableBuilderA
      .area("test_dependency_area")
      .vertical("test_vertical")
      .table("tableA")
      .version("testVersion")
      .parallelism(1)
      .maxBulkSize(1)
      .partition
      .unit("minutes")
      .size(20)
      .done
      .source
      .connectionName(singleSourceTestConnection.getName)
      .path("/further/disk/path/tableASource")
      .format("parquet")
      .version("testVersion")
      .partition
      .unit("minutes")
      .size(20)
      .done
      .select
      .query("SELECT * FROM <path> WHERE created_at >= '<lower_bound>' and created_at \\< '<upper_bound>';")
      .column("created_at")
      .done
      .done
      .target
      .connectionName(singleDataLakeConnection.getName)
      .format("parquet")
      .done
      .build
    val saved = metaStore.tableService.save(tableA, false)
    table = saved.getNewResult
  }

  override protected def afterEach(): Unit = {
    try super.afterEach()
    val db =
      DriverManager.getConnection(metastoreUrl, "UHRWERK_USER", "Xq92vFqEKF7TB8H9")
    val deleteDependencyStm = db.createStatement
    deleteDependencyStm.execute("DELETE FROM DEPENDENCY")
    deleteDependencyStm.close()
    val deleteSourceStm = db.createStatement // In case of some lost source data
    deleteSourceStm.execute("DELETE FROM SOURCE")
    deleteSourceStm.close()
    val deleteTargetStm = db.createStatement
    deleteTargetStm.execute("DELETE FROM TARGET")
    deleteTargetStm.close()
    val deleteConnectionStm = db.createStatement
    deleteConnectionStm.execute("DELETE FROM CONNECTION")
    deleteConnectionStm.close()
    val deleteTableStm = db.createStatement
    deleteTableStm.execute("DELETE FROM TABLE_")
    deleteTableStm.close()
    val deletePartitionStm = db.createStatement
    deletePartitionStm.execute("DELETE FROM PARTITION_")
    deletePartitionStm.close()
    if (db != null) {
      if (!db.isClosed) {
        db.close()
      }
    }
  }

  "A tableWrapper without written partitions" should "return no last partition timestamp" in {
    val wrapper = new TableWrapper(metaStore, table, x => TaskOutput(x.loadedInputFrames.head._2), null)
    val res = wrapper.getTimeLatestPartition()
    assert(res.isEmpty)
  }

  "A tableWrapper with some last partition" should "return the last one that was saved" in {
    val wrapper = new TableWrapper(metaStore, table, x => TaskOutput(x.loadedInputFrames.head._2), null)
    val requestedTimes = List(
      LocalDateTime.of(2020, 4, 10, 14, 0),
      LocalDateTime.of(2020, 4, 10, 14, 20),
      LocalDateTime.of(2020, 4, 10, 16, 40),
    )
    requestedTimes.foreach(dt => {
      val part = new Partition
      part.setPartitioned(true)
      part.setPartitionSize(20)
      part.setPartitionUnit(PartitionUnit.MINUTES)
      part.setPartitionTs(dt)
      part.setTargetId(table.getTargets().head.getId)
      part.setKey()
      val savePartA = metaStore.partitionService.save(part, false)
      assert(savePartA.isSuccess)
    })
    val res = wrapper.getTimeLatestPartition()
    assert(requestedTimes.last === res.get)
  }

}
