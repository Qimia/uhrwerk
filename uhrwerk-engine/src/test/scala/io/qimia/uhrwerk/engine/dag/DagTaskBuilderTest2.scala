package io.qimia.uhrwerk.engine.dag

import io.qimia.uhrwerk.common.metastore.model.{MetastoreModel => MetastoreConnInfo}
import io.qimia.uhrwerk.common.tools.TimeTools
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

  "Building a dag taskmap from a table" should "result in only those dependencies' tasks needed for that table" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    List("EnvTableTest1.yml", "EnvTableTest2.yml", "EnvTableTest3.yml", "EnvTableTest4.yml").foreach(
      env.addTableFile(_, identityUserFunc))
    val wrap3   = env.getTable(new TableIdent("test", "test_db", "tab3", "1.0")).get
    val startTs = LocalDateTime.of(2018, 6, 20, 10, 0)
    val endTs   = LocalDateTime.of(2018, 6, 20, 16, 0)

    val builder = new DagTaskBuilder2(env)
    val taskmap = builder.buildTaskListFromTable(wrap3, startTs, endTs)
    assert(taskmap.size == 3 * 6)
    val endTaskKey = DagTask2Key(new TableIdent("test", "test_db", "tab3", "1.0"), LocalDateTime.of(2018, 6, 20, 13, 0))
    val someEndTask =
      taskmap(endTaskKey)
    assert(someEndTask.upstreamDependencies.isEmpty)
    assert(someEndTask.partitions.length == 1)
    val depKeys = Set(
      DagTask2Key(new TableIdent("test", "test_db", "tab1", "1.0"), LocalDateTime.of(2018, 6, 20, 13, 0)),
      DagTask2Key(new TableIdent("test", "test_db", "tab2", "1.0"), LocalDateTime.of(2018, 6, 20, 13, 0))
    )
    assert(someEndTask.missingDependencies == depKeys)
    depKeys.foreach(k => {
      val task = taskmap(k)
      assert(task.missingDependencies.isEmpty)
      assert(task.upstreamDependencies.contains(endTaskKey))
      assert(task.partitions.length == 1)
    })

    val wrap4           = env.getTable(new TableIdent("test", "test_db", "tab4", "1.0")).get
    val endTs2          = LocalDateTime.of(2018, 6, 20, 12, 0)
    val windowedTaskmap = builder.buildTaskListFromTable(wrap4, startTs, endTs2)
    assert(windowedTaskmap.size == ((2 * 1) + (2 * 3) + 2))
    assert(!windowedTaskmap.contains(endTaskKey))

    val endTableIdent = new TableIdent("test", "test_db", "tab4", "1.0")
    val endTaskKeys = DagTask2Key(endTableIdent, LocalDateTime.of(2018, 6, 20, 10, 0)) :: DagTask2Key(
      endTableIdent,
      LocalDateTime.of(2018, 6, 20, 11, 0)) :: Nil
    endTaskKeys.foreach(k => {
      val task = windowedTaskmap(k)
      assert(task.upstreamDependencies.isEmpty)
      assert(task.partitions.length == 1)
      assert(task.missingDependencies.size == 6)
    })
    val doubleUpstreamKeys = Set(
      DagTask2Key(new TableIdent("test", "test_db", "tab1", "1.0"), LocalDateTime.of(2018, 6, 20, 9, 0)),
      DagTask2Key(new TableIdent("test", "test_db", "tab1", "1.0"), LocalDateTime.of(2018, 6, 20, 10, 0)),
      DagTask2Key(new TableIdent("test", "test_db", "tab2", "1.0"), LocalDateTime.of(2018, 6, 20, 9, 0)),
      DagTask2Key(new TableIdent("test", "test_db", "tab2", "1.0"), LocalDateTime.of(2018, 6, 20, 10, 0))
    )
    doubleUpstreamKeys.foreach(k => {
      val task = windowedTaskmap(k)
      assert(task.upstreamDependencies.size == 2)
      assert(task.partitions.length == 1)
      assert(task.missingDependencies.isEmpty)
    })
    val singleUpstreamKey = DagTask2Key(new TableIdent("test", "test_db", "tab1", "1.0"), LocalDateTime.of(2018, 6, 20, 8, 0))
    val singleUpTask      = windowedTaskmap(singleUpstreamKey)
    assert(singleUpTask.upstreamDependencies.size == 1)
    assert(singleUpTask.partitions.length == 1)
    assert(singleUpTask.missingDependencies.isEmpty)
  }

  "Building a dag taskmap for a table with a partitionless dependency" should
    "generate a deduplicated taskmap" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    val wrappers =
      List("EnvTableTest1.yml", "EnvTableTest5.yml", "EnvTableTest6.yml").map(env.addTableFile(_, identityUserFunc))
    val startTs = LocalDateTime.of(2018, 6, 20, 12, 0)
    val endTs   = LocalDateTime.of(2018, 6, 21, 0, 0)

    val builder = new DagTaskBuilder2(env)
    val taskmap = builder.buildTaskListFromTable(wrappers.last.get, startTs, endTs)
    assert(taskmap.size === 1 + (2 * 12))
  }

  "An environment with unpartitioned target table" should "generate a taskmap and with no duplicates" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    val wrappers =
      List("EnvTableTest1.yml", "EnvTableTest5.yml", "EnvTableTest7.yml").map(env.addTableFile(_, identityUserFunc))
    val startTs = LocalDateTime.of(2018, 6, 20, 12, 0)
    val endTs   = LocalDateTime.of(2018, 6, 21, 0, 0)

    val builder = new DagTaskBuilder2(env)
    val needed  = builder.buildTaskListFromTable(wrappers.last.get, startTs, endTs)

    assert(needed.size == 2)
  }

  "a dag-taskmap with bulking opportunities" should "be optimized to 'bulked' tasks" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    val wrappers =
      List("EnvTableTest1.yml", "EnvTableTest5.yml", "EnvTableTest6.yml").map(env.addTableFile(_, identityUserFunc))
    val startTs = LocalDateTime.of(2018, 6, 20, 12, 0)
    val endTs   = LocalDateTime.of(2018, 6, 21, 0, 0)

    val builder          = new DagTaskBuilder2(env)
    val taskmap          = builder.buildTaskListFromTable(wrappers.last.get, startTs, endTs)
    val optimizedTaskmap = DagTaskBuilder2.bulkOptimizeTaskmap(taskmap)
    assert(optimizedTaskmap.size === (1 + (2 * 12))) // The amount of keys stays the same in the map

    val middleTs      = LocalDateTime.of(2018, 6, 20, 18, 0)
    val lastTableWrap = wrappers.last.get
    val tab6Ident     = Environment.getTableIdent(lastTableWrap.wrappedTable)
    val bulk1Times = TimeTools
      .convertRangeToBatch(startTs, middleTs, lastTableWrap.tableDuration)
    bulk1Times
      .map(t => DagTask2Key(tab6Ident, t))
      .sliding(2)
      .foreach(twoKeys => {
        val task1 = optimizedTaskmap(twoKeys.head)
        val task2 = optimizedTaskmap(twoKeys.last)
        assert(task1 eq task2)
        assert(task1.upstreamDependencies.isEmpty)
        assert(task1.missingDependencies.size === 7)
        assert(task1.partitions.size === 6)
      })
    val bulk2Times = TimeTools
      .convertRangeToBatch(middleTs, endTs, lastTableWrap.tableDuration)
    bulk2Times
      .map(t => DagTask2Key(tab6Ident, t))
      .sliding(2)
      .foreach(twoKeys => {
        val task1 = optimizedTaskmap(twoKeys.head)
        val task2 = optimizedTaskmap(twoKeys.last)
        assert(task1 eq task2)
        assert(task1.upstreamDependencies.isEmpty)
        assert(task1.missingDependencies.size === 7)
        assert(task1.partitions.size === 6)
      })
  }

  "An environment with no optimization opportunities" should "leave all the tasks intact" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    List("EnvTableTest1.yml", "EnvTableTest2.yml", "EnvTableTest3.yml", "EnvTableTest4.yml").foreach(
      env.addTableFile(_, identityUserFunc))
    val wrap3   = env.getTable(new TableIdent("test", "test_db", "tab3", "1.0")).get
    val startTs = LocalDateTime.of(2018, 6, 20, 10, 0)
    val endTs   = LocalDateTime.of(2018, 6, 20, 16, 0)

    val builder          = new DagTaskBuilder2(env)
    val taskmap          = builder.buildTaskListFromTable(wrap3, startTs, endTs)
    val optimizedTaskmap = DagTaskBuilder2.bulkOptimizeTaskmap(taskmap)
    optimizedTaskmap.foreach(kv => assert(kv._2.partitions.size === 1))
  }

  "An environment with multiple layers tables with optimization opportunities" should
    "still return a valid dag-taskmap" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    List(1, 8, 9).map(n => s"EnvTableTest${n.toString}.yml").foreach(env.addTableFile(_, identityUserFunc))

    val targetIdent = new TableIdent("test", "test_db", "tab9", "1.0")
    val startTs     = LocalDateTime.of(2010, 4, 10, 6, 0)
    val midpointTs  = LocalDateTime.of(2010, 4, 10, 9, 0)
    val endTs       = LocalDateTime.of(2010, 4, 10, 12, 0)

    val targetWrap       = env.getTable(targetIdent).get
    val builder          = new DagTaskBuilder2(env)
    val taskmap          = builder.buildTaskListFromTable(targetWrap, startTs, endTs)
    val optimizedTaskmap = DagTaskBuilder2.bulkOptimizeTaskmap(taskmap)

    def check2Key(input: List[DagTask2Key]): Unit = {
      val task1 = optimizedTaskmap(input.head)
      val task2 = optimizedTaskmap(input.last)
      assert(task1 eq task2)
      assert(task1.upstreamDependencies.isEmpty)
      assert(task1.missingDependencies.size === (3 + (3 * 4)))
      assert(task1.partitions.size === 3)
    }
    val bulk1Times = TimeTools
      .convertRangeToBatch(startTs, midpointTs, targetWrap.tableDuration)
    bulk1Times
      .map(t => DagTask2Key(targetIdent, t))
      .sliding(2)
      .foreach(check2Key)
    val bulk2Times = TimeTools
      .convertRangeToBatch(midpointTs, endTs, targetWrap.tableDuration)
    bulk2Times
      .map(t => DagTask2Key(targetIdent, t))
      .sliding(2)
      .foreach(check2Key)

    val aggregateDepIdent = new TableIdent("test", "test_db", "tab8", "1.0")
    val aggregateDepWrap  = env.getTable(aggregateDepIdent).get
    val quarterTimes = TimeTools
      .convertRangeToBatch(startTs, endTs, aggregateDepWrap.tableDuration)
    quarterTimes
      .map(t => DagTask2Key(aggregateDepIdent, t))
      .sliding(4, 4)
      .foreach(quadList => {
        val task1 = optimizedTaskmap(quadList.head)
        val task2 = optimizedTaskmap(quadList(1))
        val task3 = optimizedTaskmap(quadList(2))
        val task4 = optimizedTaskmap(quadList.last)
        assert(task1 eq task2)
        assert(task1 eq task3)
        assert(task1 eq task4)
        assert(task1.partitions.size === 4)
        assert(task1.missingDependencies.isEmpty)
        // Every 4 partitions only end up in a single target table partition
        assert(task1.upstreamDependencies.size === 1)
        val upstreamTask = optimizedTaskmap(task1.upstreamDependencies.head)
        assert(upstreamTask.partitions.size === 3)
      })
    val diffTask1 = optimizedTaskmap(DagTask2Key(aggregateDepIdent, LocalDateTime.of(2010, 4, 10, 7, 45)))
    val diffTask2 = optimizedTaskmap(DagTask2Key(aggregateDepIdent, LocalDateTime.of(2010, 4, 10, 8, 0)))
    // Tasks are only combined per 4 so not across hour
    assert(diffTask1 ne diffTask2)
  }
}
