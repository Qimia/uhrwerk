package io.qimia.uhrwerk.engine.dag

import io.qimia.uhrwerk.common.model.{Metastore => MetastoreConnInfo}
import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{Environment, MetaStore, TaskInput, TaskOutput}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.DriverManager
import java.time.LocalDateTime

class DagTaskBuilderTest2 extends AnyFlatSpec with BeforeAndAfterEach {

  val metastoreUrl = "jdbc:mysql://localhost:53306/UHRWERK_METASTORE_UNIT_TESTS"

  val testConnInfo = new MetastoreConnInfo
  testConnInfo.setJdbc_url(metastoreUrl)
  testConnInfo.setJdbc_driver("com.mysql.cj.jdbc.Driver")
  testConnInfo.setUser("UHRWERK_USER")
  testConnInfo.setPass("Xq92vFqEKF7TB8H9")
  val metaStore: MetaStore = MetaStore.build(testConnInfo)

  override protected def afterEach(): Unit = {
    super.afterEach()
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
    List("EnvTableTest1.yml", "EnvTableTest2.yml", "EnvTableTest3.yml", "EnvTableTest4.yml").foreach(
      env.addTableFile(_, identityUserFunc))
    val wrap3   = env.getTable(TableIdent("test", "test_db", "tab3", "1.0")).get
    val startTs = LocalDateTime.of(2018, 6, 20, 10, 0)
    val endTs   = LocalDateTime.of(2018, 6, 20, 16, 0)

    val builder = new DagTaskBuilder2(env)
    val taskmap = builder.buildTaskListFromTable(wrap3, startTs, endTs)
    assert(taskmap.size == 3 * 6)
    val endTaskKey = DT2Key(TableIdent("test", "test_db", "tab3", "1.0"), LocalDateTime.of(2018, 6, 20, 13, 0))
    val someEndTask =
      taskmap(endTaskKey)
    assert(someEndTask.upstreamDependencies.isEmpty)
    assert(someEndTask.partitions.length == 1)
    val depKeys = Set(
      DT2Key(TableIdent("test", "test_db", "tab1", "1.0"), LocalDateTime.of(2018, 6, 20, 13, 0)),
      DT2Key(TableIdent("test", "test_db", "tab2", "1.0"), LocalDateTime.of(2018, 6, 20, 13, 0))
    )
    assert(someEndTask.missingDependencies == depKeys)
    depKeys.foreach(k => {
      val task = taskmap(k)
      assert(task.missingDependencies.isEmpty)
      assert(task.upstreamDependencies.contains(endTaskKey))
      assert(task.partitions.length == 1)
    })

    val wrap4           = env.getTable(TableIdent("test", "test_db", "tab4", "1.0")).get
    val endTs2          = LocalDateTime.of(2018, 6, 20, 12, 0)
    val windowedTaskmap = builder.buildTaskListFromTable(wrap4, startTs, endTs2)
    assert(windowedTaskmap.size == ((2 * 1) + (2 * 3) + 2))
    assert(!windowedTaskmap.contains(endTaskKey))

    val endTableIdent = TableIdent("test", "test_db", "tab4", "1.0")
    val endTaskKeys = DT2Key(endTableIdent, LocalDateTime.of(2018, 6, 20, 10, 0)) :: DT2Key(
      endTableIdent,
      LocalDateTime.of(2018, 6, 20, 11, 0)) :: Nil
    endTaskKeys.foreach(k => {
      val task = windowedTaskmap(k)
      assert(task.upstreamDependencies.isEmpty)
      assert(task.partitions.length == 1)
      assert(task.missingDependencies.size == 6)
    })
    val doubleUpstreamKeys = Set(
      DT2Key(TableIdent("test", "test_db", "tab1", "1.0"), LocalDateTime.of(2018, 6, 20, 9, 0)),
      DT2Key(TableIdent("test", "test_db", "tab1", "1.0"), LocalDateTime.of(2018, 6, 20, 10, 0)),
      DT2Key(TableIdent("test", "test_db", "tab2", "1.0"), LocalDateTime.of(2018, 6, 20, 9, 0)),
      DT2Key(TableIdent("test", "test_db", "tab2", "1.0"), LocalDateTime.of(2018, 6, 20, 10, 0))
    )
    doubleUpstreamKeys.foreach(k => {
      val task = windowedTaskmap(k)
      assert(task.upstreamDependencies.length == 2)
      assert(task.partitions.length == 1)
      assert(task.missingDependencies.isEmpty)
    })
    val singleUpstreamKey = DT2Key(TableIdent("test", "test_db", "tab1", "1.0"), LocalDateTime.of(2018, 6, 20, 8, 0))
    val singleUpTask      = windowedTaskmap(singleUpstreamKey)
    assert(singleUpTask.upstreamDependencies.length == 1)
    assert(singleUpTask.partitions.length == 1)
    assert(singleUpTask.missingDependencies.isEmpty)
  }
}
