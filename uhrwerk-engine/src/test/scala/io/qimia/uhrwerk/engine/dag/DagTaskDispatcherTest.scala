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

class DagTaskDispatcherTest extends AnyFlatSpec with BeforeAndAfterEach {

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

  def cleanDB(): Unit = {
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
  }
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cleanDB()
  }
  override protected def afterEach(): Unit = {
    super.afterEach()
    cleanDB()
  }

  def identityUserFunc(name: String): TaskInput => TaskOutput = {
    val e = (x: TaskInput) => {
      println(name)
      TaskOutput(x.loadedInputFrames.values.head)
    }
    e
  }

  "Building a dag from a dag file" should "result in only those dependencies needed by the target tables" in {
    val frameManager = new SparkFrameManager(sparkSess)
    val env          = new Environment(metaStore, frameManager)
    //env.addConnectionFile("EnvTableConn1.yml")
    var mappedIdentityUserFunc = collection.mutable.Map[TableIdent, TaskInput => TaskOutput]()
    mappedIdentityUserFunc += (TableIdent("loading", "dtd", "table_s", "1.0") -> identityUserFunc("table_s"))

    val level1_tables = ('a' until 'i').toList.map(x => {
      (TableIdent("loading", "dtd", f"table_1$x", "1.0") -> identityUserFunc(f"table_1$x"))
    })

    val identityFuncsWithIndentifier =
      (TableIdent("loading", "dtd", "table_3a", "1.0")   -> identityUserFunc("table_3a")) ::
        (TableIdent("loading", "dtd", "table_3b", "1.0") -> identityUserFunc("table_3b")) ::
        (TableIdent("loading", "dtd", "table_2a", "1.0") -> identityUserFunc("table_2a")) ::
        (TableIdent("loading", "dtd", "table_2b", "1.0") -> identityUserFunc("table_2b")) ::
        level1_tables

    mappedIdentityUserFunc = mappedIdentityUserFunc ++ identityFuncsWithIndentifier
    env.setupDagFile("DagTaskDispatcherTest.yml", mappedIdentityUserFunc.toMap, overwrite = true)

    val wrap1   = env.getTable(TableIdent("loading", "dtd", "table_3b", "1.0")).get
    val startTs = LocalDateTime.of(2018, 6, 20, 0, 0)
    val endTs   = LocalDateTime.of(2018, 6, 21, 0, 0)
    val builder = new DagTaskBuilder2(env)
    val needed  = builder.buildTaskListFromTable(wrap1, startTs, endTs)

    DagTaskDispatcher.runTasksParallelWithFullDAGGeneration(needed, 1)
  }

}
