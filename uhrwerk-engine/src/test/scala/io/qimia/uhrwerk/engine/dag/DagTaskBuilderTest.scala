package io.qimia.uhrwerk.engine.dag

import java.sql.DriverManager
import java.time.LocalDateTime

import io.qimia.uhrwerk.common.model.{Metastore => MetastoreConnInfo}
import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{Environment, MetaStore, TaskInput, TaskOutput}
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

class DagTaskBuilderTest extends AnyFlatSpec with BeforeAndAfterEach {

  val metastoreUrl = "jdbc:mysql://localhost:53306/UHRWERK_METASTORE_UNIT_TESTS"

  val testConnInfo = new MetastoreConnInfo
  testConnInfo.setJdbc_url(metastoreUrl)
  testConnInfo.setJdbc_driver("com.mysql.cj.jdbc.Driver")
  testConnInfo.setUser("UHRWERK_USER")
  testConnInfo.setPass("Xq92vFqEKF7TB8H9")
  val metaStore = MetaStore.build(testConnInfo)

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
    if (db != null) {
      if (!db.isClosed) {
        db.close()
      }
    }
  }

  def identityUserFunc(in: TaskInput): TaskOutput = {
    TaskOutput(in.loadedInputFrames.values.head)
  }

  "Building a dag from a table" should "result in only those dependencies needed by a table" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    val wrappers =
      List("EnvTableTest1.yml", "EnvTableTest2.yml", "EnvTableTest3.yml", "EnvTableTest4.yml").map(env.addTableFile(_, identityUserFunc))
    val wrap3 = env.getTable(TableIdent("test", "test_db", "tab3", "1.0")).get
    val startTs = LocalDateTime.of(2018, 6, 20, 10, 0)
    val endTs = LocalDateTime.of(2018, 6, 20, 16, 0)

    val builder = new DagTaskBuilder(env)
    var needed = builder.buildTaskListFromTable(wrap3, startTs, endTs)
    assert(needed.size == 3 * 6)
    needed.foreach(tup => tup.partitions.size == 1)
    assert(needed.last.table.wrappedTable.getName === "tab3")

    val wrap4 = env.getTable(TableIdent("test", "test_db", "tab4", "1.0")).get
    needed = builder.buildTaskListFromTable(wrap4, startTs, endTs)
    assert(needed.size == 3 * 6)
    val depNeeded = needed.filter(t => t.table.wrappedTable.getName != "tab4")
    depNeeded.foreach(tup => assert(tup.partitions.size == 3))
  }

  "Building a dag from a dag file" should "result in only those dependencies needed by the target tables" in {
    val env = new Environment(metaStore, null)
    //env.addConnectionFile("EnvTableConn1.yml")
    var mappedIdentityUserFunc = collection.mutable.Map[TableIdent, TaskInput => TaskOutput]()
    mappedIdentityUserFunc += (TableIdent("loading","sourcedb_1","load_a_table1","1.0") -> identityUserFunc )
    mappedIdentityUserFunc += (TableIdent("loading","sourcedb_1","load_a_table2","1.0") -> identityUserFunc )
    mappedIdentityUserFunc += (TableIdent("loading","sourcedb_1","load_a_table3","1.0") -> identityUserFunc )
    mappedIdentityUserFunc += (TableIdent("processing","sourcedb_1","process_a_table1","1.0") -> identityUserFunc )
    mappedIdentityUserFunc += (TableIdent("processing","sourcedb_1","process_a_table2","1.0") -> identityUserFunc )
    mappedIdentityUserFunc += (TableIdent("processing","sourcedb_1","process_a_table3","1.0") -> identityUserFunc )
    mappedIdentityUserFunc += (TableIdent("final","sourcedb_1","finalize_a_table1","1.0") -> identityUserFunc )
    env.setupDagFile("EnvDagTest1.yml", mappedIdentityUserFunc.toMap, true)
    val wrap1 = env.getTable(TableIdent("processing", "sourcedb_1", "process_a_table1", "1.0")).get
    val startTs = LocalDateTime.of(2018, 6, 20, 10, 0)
    val endTs = LocalDateTime.of(2018, 6, 20, 16, 0)
    val dayEndTs = LocalDateTime.of(2018, 6, 26, 10, 0)
    val builder = new DagTaskBuilder(env)
    val needed = builder.buildTaskListFromTable(wrap1, startTs, endTs)

    assert(needed.size == 2 * 2 ) //There are 6 partitions for 2 tables, but the six partitions are bulked in two groups
    assert(needed.head.partitions.size == 4)

    val wrap4 = env.getTable(TableIdent("processing", "sourcedb_1", "process_a_table2", "1.0")).get
    val needed2 = builder.buildTaskListFromTable(wrap4, startTs, endTs)

    assert(DagTaskBuilder.distinctDagTasks(needed2).length == 2)

    val wrap5 = env.getTable(TableIdent("final", "sourcedb_1", "finalize_a_table1", "1.0")).get
    val needed3 = builder.buildTaskListFromTable(wrap5, startTs, dayEndTs)

    assert(DagTaskBuilder.distinctDagTasks(needed3).length == 6)

  }

  "A filled environment with target table" should "generate a task-queue and deduplicate unpartitioned" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    val wrappers =
      List("EnvTableTest1.yml", "EnvTableTest5.yml", "EnvTableTest6.yml").map(env.addTableFile(_, identityUserFunc))
    val startTs = LocalDateTime.of(2018, 6, 20, 12, 0)
    val endTs = LocalDateTime.of(2018, 6, 21, 0, 0)

    val builder = new DagTaskBuilder(env)
    var needed = builder.buildTaskListFromTable(wrappers.last.get, startTs, endTs)
    assert(needed.length === 3 * (12 / 6))
    val depduplicated = DagTaskBuilder.distinctDagTasks(needed)
    assert(depduplicated.length === 1 + (2 * (12 / 6)))
  }

  "A filled environment with unpartitioned target table" should "generate a task-queue and deduplicate unpartitioned" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    val wrappers =
      List("EnvTableTest1.yml", "EnvTableTest5.yml", "EnvTableTest7.yml").map(env.addTableFile(_, identityUserFunc))
    val startTs = LocalDateTime.of(2018, 6, 20, 12, 0)
    val endTs = LocalDateTime.of(2018, 6, 21, 0, 0)

    val builder = new DagTaskBuilder(env)
    var needed = builder.buildTaskListFromTable(wrappers.last.get, startTs, endTs)

    val depduplicated = DagTaskBuilder.distinctDagTasks(needed)
    assert(depduplicated.length == 2)

  }

}
