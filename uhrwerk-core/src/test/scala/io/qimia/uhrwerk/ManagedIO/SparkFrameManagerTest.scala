package io.qimia.uhrwerk.ManagedIO

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration, LocalDateTime}
import java.util.Comparator

import io.qimia.uhrwerk.models.config.{Connection, Target}
import io.qimia.uhrwerk.utils.{Converters, JDBCTools, TimeTools}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

trait BuildTeardown extends BeforeAndAfterAll {
  this: Suite =>

  val foldersToClean: ListBuffer[Path] = new ListBuffer[Path]

  override def afterAll() {
    try super.afterAll() // To be stackable, must call super.afterEach
    finally foldersToClean.foreach(p => {
      cleanFolder(p)
    })
  }

  def cleanFolder(location: Path): Unit = {
    Files
      .walk(location)
      .sorted(Comparator.reverseOrder())
      .iterator()
      .asScala
      .map(_.toFile)
      .foreach(f => f.delete())
  }
}

class SparkFrameManagerTest extends AnyFlatSpec with BuildTeardown {

  def getSparkSession: SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("TestFrameManager2")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    spark
  }

  "concatenatePaths" should "properly concatenate paths" in {
    val a = "a"
    val b = "b"
    val c = "/c"
    val d = "d/"
    val e = "/e/"

    val result = SparkFrameManager.concatenatePaths(a, b, c, d, e)
    assert(result === "a/b/c/d/e")
  }

  "safe DF to Lake and load from lake" should "return the same df and create the right folderstructure" in {
    val spark = getSparkSession
    import spark.implicits._

    val df = (1 to 100).map(i => (i, "txt", i * 5)).toDF("a", "b", "c")
    val manager = new SparkFrameManager(spark)

    val conn = new Connection
    conn.setName("testSparkFrameManager")
    conn.setType("fs")
    conn.setStartPath("src/test/resources/testlake/")
    val tar = new Target
    tar.setPartitionSize("30m")
    tar.setConnectionName("testSparkFrameManager")
    tar.setPath("testsparkframemanager")
    tar.setVersion(1)
    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)
    manager.writeDFToLake(df, conn, tar, Option(dateTime))

    val dep = Converters.convertTargetToDependency(tar)
    val loadedDF = manager.loadDFFromLake(conn, dep, Option(dateTime))

    val a = df.collect()
    val b = loadedDF.collect()
    assert(a === b)

    assert(
      new File("src/test/resources/testlake/testsparkframemanager/" +
        "date=2020-02-04/batch=2020-02-04-10-1").isDirectory)

    foldersToClean.append(Paths.get("src/test/resources/testlake/testsparkframemanager"))
  }

  def generateDF(spark: SparkSession, duration: Duration, dateTime: LocalDateTime): DataFrame = {
    import spark.implicits._
    val year = dateTime.getYear
    val month = dateTime.getMonthValue
    val day = dateTime.getDayOfMonth
    val hour = dateTime.getHour
    val batch = TimeTools.getBatchInHourNumber(dateTime.toLocalTime, duration)
    (1 to 50).map(i => (i, year, month, day, hour, batch)).toDF("i", "year", "month", "day", "hour", "bInHour")
  }

  "load DFs" should "result in quick access to a range of batches" in {
    val spark = getSparkSession
    import spark.implicits._
    val manager = new SparkFrameManager(spark)
    val conn = new Connection
    conn.setName("testSparkRange")
    conn.setType("fs")
    conn.setStartPath("src/test/resources/testlake/")
    val tar = new Target
    tar.setPartitionSize("30m")
    tar.setConnectionName("testSparkRange")
    tar.setPath("testsparkframerange")
    tar.setVersion(1)
    val tarDuration = tar.getPartitionSizeDuration

    val batchDates = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2015, 10, 12, 0, 0),
      LocalDateTime.of(2015, 10, 15, 0, 0),
      tarDuration
    )
    batchDates.foreach(bd => {
      println(bd)
      val df = generateDF(spark, tarDuration, bd)
      manager.writeDFToLake(df, conn, tar, Option(bd))
    })

    val dep = Converters.convertTargetToDependency(tar)
    val bigDF = manager.loadDFsFromLake(
      conn,
      dep,
      LocalDateTime.of(2015, 10, 12, 20, 0),
      LocalDateTime.of(2015, 10, 13, 20, 0),
      tarDuration
    )
    val results = bigDF.agg(min($"batch"), max($"batch")).collect().head
    assert(results.getAs[String](0) === "2015-10-12-20-0")
    assert(results.getAs[String](1) === "2015-10-13-19-1")
    foldersToClean.append(Paths.get("src/test/resources/testlake/testsparkframerange/"))
  }

  "safe DF to jdbc and load from jdbc" should "return the same df and create the table in the db" in {
    val spark = getSparkSession
    import spark.implicits._

    val df = (1 to 100).map(i => (i, "txt", i * 5)).toDF("a", "b", "c")
    val manager = new SparkFrameManager(spark)

    val conn = new Connection
    conn.setName("testSparkJDBCFrameManager")
    conn.setType("jdbc")
    conn.setJdbcDriver("com.mysql.cj.jdbc.Driver")
    conn.setJdbcUri("jdbc:mysql://localhost:43342")
    conn.setUser("root")
    conn.setPass("mysql")
    val tar = new Target
    tar.setPartitionSize("30m")
    tar.setConnectionName("testSparkJDBCFrameManager")
    tar.setPath("testsparkframemanager")
    tar.setVersion(1)
    tar.setExternal(false)
    tar.setArea("uhrwerk_test")
    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)
    manager.writeDFToLake(df, conn, tar, Option(dateTime))

    val dep = Converters.convertTargetToDependency(tar)
    val loadedDF = manager.loadDFFromLake(conn, dep, Option(dateTime)).sort("a", "b", "c")

    val a = df.collect()
    val b = loadedDF.collect()
    assert(a === b)

    JDBCTools.dropJDBCDatabase(conn, tar)
  }
}
