package io.qimia.uhrwerk.engine.dag

import java.sql.DriverManager
import java.time.LocalDateTime

import io.qimia.uhrwerk.common.model.{Metastore => MetastoreConnInfo}
import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{Environment, MetaStore, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import matchers.should.Matchers._
import org.scalatest.matchers.should

import scala.collection.mutable.ListBuffer


class DagTaskDispatcher2Test extends AnyFlatSpec with BeforeAndAfterEach {

  val sparkSess = SparkSession
    .builder()
    .appName(this.getClass.toString)
    .master("local[3]")
    .getOrCreate()

  val metastoreUrl = "jdbc:mysql://localhost:53306/UHRWERK_METASTORE_UNIT_TESTS"

  val testConnInfo = new MetastoreConnInfo
  testConnInfo.setJdbc_url(metastoreUrl)
  testConnInfo.setJdbc_driver("com.mysql.cj.jdbc.Driver")
  testConnInfo.setUser("UHRWERK_USER")
  testConnInfo.setPass("Xq92vFqEKF7TB8H9")
  val metaStore: MetaStore = MetaStore.build(testConnInfo)


  def cleanMetastore(): Unit = {
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
    val deletePartDep = db.createStatement
    deletePartDep.execute("DELETE FROM PARTITION_DEPENDENCY")
    deletePartDep.close()
    if (db != null) {
      if (!db.isClosed) {
        db.close()
      }
    }
    tasksRan = new ListBuffer[String]()
    taskLogs = new ListBuffer[String]()
    tasksFailed = new ListBuffer[String]()
  }


  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cleanMetastore()
  }


  override protected def afterEach(): Unit = {
    super.afterEach()
    cleanMetastore()
  }

  def identityUserFunc(name: String, waitMillis:Int=0): TaskInput => TaskOutput = {
    val e = (x: TaskInput) => {
      synchronized(tasksRan) {
        tasksRan +=  name;
        0}
      synchronized(taskLogs){taskLogs += f"Started $name"; 0}
      Thread.sleep(waitMillis)
      val r = TaskOutput(x.loadedInputFrames.values.head)
      synchronized(taskLogs){taskLogs += f"BeforeFinish $name"; 0}
      r
    }
    e
  }
  def identityUserFuncWithFailure(name: String, waitMillis:Int=0): TaskInput => TaskOutput = {
    val e = (x: TaskInput) => {
      synchronized(tasksFailed) {
        tasksFailed +=  name;
        0}
      synchronized(taskLogs){taskLogs += f"Started $name"; 0}
      Thread.sleep(waitMillis)
      throw new Exception("A mock exception")
      val r = TaskOutput(x.loadedInputFrames.values.head)
      synchronized(taskLogs){taskLogs += f"BeforeFinish $name"; 0}
      r
    }
    e
  }

  var taskLogs = new ListBuffer[String]()
  var tasksRan:ListBuffer[String] = new ListBuffer[String]()
  var tasksFailed:ListBuffer[String] = new ListBuffer[String]()

  def getEnv(waitMillis:Int=0)={
    val frameManager = new SparkFrameManager(sparkSess)
    val env          = new Environment(metaStore, frameManager)
    //env.addConnectionFile("EnvTableConn1.yml")
    var mappedIdentityUserFunc = collection.mutable.Map[TableIdent, TaskInput => TaskOutput]()
    mappedIdentityUserFunc += (TableIdent("loading", "dtd", "table_s", "1.0") -> identityUserFunc("table_s", waitMillis=waitMillis))

    val level1_tables = ('a' until 'i').toList.map(x => {
      (TableIdent("loading", "dtd", f"table_1$x", "1.0") -> identityUserFunc(f"table_1$x", waitMillis=waitMillis))
    })

    val identityFuncsWithIndentifier =
      (TableIdent("loading", "dtd", "table_3a", "1.0")   -> identityUserFunc("table_3a", waitMillis=waitMillis)) ::
        (TableIdent("loading", "dtd", "table_3b", "1.0") -> identityUserFunc("table_3b", waitMillis=waitMillis)) ::
        (TableIdent("loading", "dtd", "table_2a", "1.0") -> identityUserFunc("table_2a", waitMillis=waitMillis)) ::
        (TableIdent("loading", "dtd", "table_2b", "1.0") -> identityUserFunc("table_2b", waitMillis=waitMillis)) ::
        (TableIdent("loading", "dtd", "table_2c", "1.0") -> identityUserFunc("table_2c", waitMillis=waitMillis)) ::
        (TableIdent("loading", "dtd", "table_4a", "1.0") -> identityUserFunc("table_4a", waitMillis=waitMillis)) ::
        (TableIdent("loading", "dtd", "table_s2", "1.0") -> identityUserFunc("table_s2", waitMillis=waitMillis)) ::
        (TableIdent("loading", "dtd", "table_2d", "1.0") -> identityUserFunc("table_2d", waitMillis=waitMillis)) ::
        (TableIdent("loading", "dtd", "table_1i", "1.0") -> identityUserFuncWithFailure("table_1i", waitMillis=waitMillis)) ::
        level1_tables

    mappedIdentityUserFunc = mappedIdentityUserFunc ++ identityFuncsWithIndentifier
    env.setupDagFile("DagTaskDispatcherTest.yml", mappedIdentityUserFunc.toMap, overwrite = true)
    env
  }


  "Building a dag from a dag file" should "result in only those dependencies needed by the target table_4a" in {

    val env = getEnv(0)
    val wrap1   = env.getTable(TableIdent("loading", "dtd", "table_4a", "1.0")).get
    val startTs = LocalDateTime.of(2018, 6, 20, 0, 0)
    val endTs   = LocalDateTime.of(2018, 6, 21, 0, 0)
    val builder = new DagTaskBuilder2(env)
    val needed  = builder.buildTaskListFromTable(wrap1, startTs, endTs)
    DagTaskDispatcher2.runTasks(needed, 2)
    val expectedTasks = (
      "table_4a"::
      "table_3a" ::
      "table_3b" ::
      "table_s2" ::
      "table_s" ::
      "table_1e" ::
      "table_2a" ::
      "table_1a" ::
      "table_2b" ::
      "table_1b" ::
      Nil
      )
    tasksRan.sortBy(x=>x) should contain theSameElementsAs expectedTasks.sortBy(x=>x)
  }

  "Building a dag from a dag file for table_2d" should "result in table_s, table_1h succeeding; table_1i, and table_2d failing" in {

    val env = getEnv(0)
    val wrap1   = env.getTable(TableIdent("loading", "dtd", "table_2d", "1.0")).get
    val startTs = LocalDateTime.of(2018, 6, 20, 0, 0)
    val endTs   = LocalDateTime.of(2018, 6, 21, 0, 0)
    val builder = new DagTaskBuilder2(env)
    val needed  = builder.buildTaskListFromTable(wrap1, startTs, endTs)
    DagTaskDispatcher2.runTasks(needed, 2)
    val expectedSuccess = (
      "table_s"::
        "table_1h" ::
        Nil)
    val expectedFailure = (
      "table_1i"::
        Nil)
    tasksRan.sortBy(x=>x) should contain theSameElementsAs expectedSuccess.sortBy(x=>x)
    tasksFailed.sortBy(x=>x) should contain theSameElementsAs expectedFailure.sortBy(x=>x)
  }

  "Building a dag with lots of tasks with the same dependency" should "result in all tasks running at the same time" in {

    /**
     * We give each task with the same dependency 20 seconds to run; therefore, they should run in parallel.
     * The latest task start should be after the earliest task start.
     */
    val env = getEnv(20000)
    val wrap1 = env.getTable(TableIdent("loading", "dtd", "table_2c", "1.0")).get
    val startTs = LocalDateTime.of(2018, 6, 20, 0, 0)
    val endTs = LocalDateTime.of(2018, 6, 21, 0, 0)
    val builder = new DagTaskBuilder2(env)
    val needed = builder.buildTaskListFromTable(wrap1, startTs, endTs)
    DagTaskDispatcher2.runTasks(needed, 20)


    val maxIndexOfTaskStart = taskLogs
      .filter(x=>x.contains("_1")).zipWithIndex.filter(x=>x._1.contains("Started")).map(_._2).max
    val minIndexOfTaskEnd = taskLogs
      .filter(x=>x.contains("_1")).zipWithIndex.filter(x=>x._1.contains("BeforeFinish")).map(_._2).min

    maxIndexOfTaskStart + 1 shouldBe minIndexOfTaskEnd
  }


}
