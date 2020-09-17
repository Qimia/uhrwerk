package io.qimia.uhrwerk.engine

import java.sql.DriverManager

import io.qimia.uhrwerk.common.model.{Metastore => MetastoreConnInfo}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

/**
  * These are large integration tests which depend on config reading, model storing,
  * and validation checking. They do **not** test the actual execution and dependency-checking
  */
class EnvironmentTableTest extends AnyFlatSpec with BeforeAndAfterEach {

  val metastoreUrl = "jdbc:mysql://localhost:53306/UHRWERK_METASTORE_UNIT_TESTS"

  val testConnInfo = new MetastoreConnInfo
  testConnInfo.setJdbc_url(metastoreUrl)
  testConnInfo.setJdbc_driver("com.mysql.cj.jdbc.Driver")
  testConnInfo.setUser("UHRWERK_USER")
  testConnInfo.setPass("Xq92vFqEKF7TB8H9")
  val metaStore = MetaStore.build(testConnInfo)

  def identityUserFunc(in: TaskInput): TaskOutput = {
    TaskOutput(in.loadedInputFrames.values.head)
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
    if (db != null) {
      if (!db.isClosed) {
        db.close()
      }
    }
  }

  "An environment without connections" should "refuse to load any tables" in {
    val env     = new Environment(metaStore, null)
    val wrapper = env.addTableFile("EnvTableTest1.yml", identityUserFunc)
    assert(wrapper.isEmpty)
  }

  it should "load when connection are provided" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    val wrapper = env.addTableFile("EnvTableTest1.yml", identityUserFunc)
    assert(wrapper.isDefined)
  }

  "A table without all its dependencies loaded" should "be rejected by the environment" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    val wrapper1 = env.addTableFile("EnvTableTest3.yml", identityUserFunc)
    assert(wrapper1.isEmpty)
    val wrapper2A = env.addTableFile("EnvTableTest1.yml", identityUserFunc)
    val wrapper2B = env.addTableFile("EnvTableTest3.yml", identityUserFunc, true)
    assert(wrapper2A.isDefined)
    assert(wrapper2B.isEmpty)
  }

  it should "be accepted when all dependencies are there" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    val wrappers =
      List("EnvTableTest1.yml", "EnvTableTest2.yml", "EnvTableTest3.yml").map(env.addTableFile(_, identityUserFunc))
    assert(wrappers.forall(_.isDefined))
  }

  // A malformed-unit config
  ignore should "be rejected" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    // FIXME: Needs validate this before loading into model pojo's
    // in model pojo's a unit of null means it is an unpartitioned table
    val wrapper = env.addTableFile("EnvTableTest1Bad1.yml", identityUserFunc)
    assert(wrapper.isEmpty)
  }

  // A not matching source-partition-size
  ignore should "not be allowed to load" in {
    // FIXME: This should result in a false config (fix is in config-branch)
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    val wrapper = env.addTableFile("EnvTableTest1Bad2.yml", identityUserFunc)
    assert(wrapper.isEmpty)
  }

  "A wrong dependency size" should "be rejected when storing" in {
    val env = new Environment(metaStore, null)
    env.addConnectionFile("EnvTableConn1.yml")
    val wrapperA = env.addTableFile("EnvTableTest1.yml", identityUserFunc)
    val wrapperB = env.addTableFile("EnvTableTest2.yml", identityUserFunc)
    assert(wrapperA.isDefined)
    assert(wrapperB.isDefined)
    val wrapperC = env.addTableFile("EnvTableTest3Bad.yml", identityUserFunc)
    assert(wrapperC.isEmpty)

    val wrapperD = env.addTableFile("EnvTableTest4Bad.yml", identityUserFunc)
    assert(wrapperD.isEmpty)
  }

}
