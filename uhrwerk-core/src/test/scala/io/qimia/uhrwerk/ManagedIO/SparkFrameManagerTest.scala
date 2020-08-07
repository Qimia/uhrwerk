package io.qimia.uhrwerk.ManagedIO

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration, LocalDateTime}
import java.util.Comparator

import io.qimia.uhrwerk.config.model._
import io.qimia.uhrwerk.tags.{DbTest, Slow}
import io.qimia.uhrwerk.utils.{Converters, JDBCTools, TimeTools}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

trait BuildTeardown extends BeforeAndAfterAll {
  this: Suite =>

  val DATABASE_NAME: String = "uhrwerk_test"

  override def afterAll() {
    val foldersToClean: ListBuffer[Path] = new ListBuffer[Path]
    foldersToClean.append(Paths.get("src/test/resources/testlake/testsparkframemanagerwithoutbatches"))
    foldersToClean.append(Paths.get("src/test/resources/testlake/testsparkframemanager"))
    foldersToClean.append(Paths.get("src/test/resources/testlake/testaggregatedependency/"))
    foldersToClean.append(Paths.get("src/test/resources/testlake/testsparkframerange/"))
    foldersToClean.append(Paths.get("src/test/resources/testlake/testwindowdependency/"))

    val conn = getJDBCConnection
    JDBCTools.dropJDBCDatabase(conn, DATABASE_NAME)
    foldersToClean.foreach(p => {
      try {
        cleanFolder(p)
      } catch {
        case exception: Exception =>
          println("Couldn't remove folder")
          println(exception.getLocalizedMessage)
      }
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

  def getJDBCConnection: Connection = {
    val conn = new Connection
    conn.setName("testSparkJDBCFrameManager")
    conn.setType("jdbc")
    conn.setJdbcDriver("com.mysql.cj.jdbc.Driver")
    conn.setJdbcUrl("jdbc:mysql://localhost:43342")
    conn.setUser("root")
    conn.setPass("mysql")
    conn
  }
}

class SparkFrameManagerTest extends AnyFlatSpec with BuildTeardown {
  def getSparkSession: SparkSession = {
    val numberOfCores = Runtime.getRuntime.availableProcessors
    val spark = SparkSession
      .builder()
      .appName("TestFrameManager2")
      .master(s"local[${numberOfCores - 1}]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    spark
  }

  def createMockDataFrame(spark: SparkSession): DataFrame = {
    import spark.implicits._
    (1 to 100).map(i => (i, "txt", i * 5)).toDF("a", "b", "c")
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

  "concatenatePaths" should "properly concatenate paths" in {
    val a = "a"
    val b = "b"
    val c = "/c"
    val d = "d/"
    val e = "/e/"

    val result = SparkFrameManager.concatenatePaths(a, b, c, d, e)
    assert(result === "a/b/c/d/e")
  }

  "save DF and load DF without batches" should "return the same df and create the right folderstructure" in {
    val spark = getSparkSession

    val df = createMockDataFrame(spark)
    val manager = new SparkFrameManager(spark)

    val conn = new Connection
    conn.setName("testSparkFrameManagerWithoutBatches")
    conn.setType("fs")
    conn.setStartPath("src/test/resources/testlake/")
    val tar = new Target
    tar.setConnectionName("testSparkFrameManagerWithoutBatches")
    tar.setPath("testsparkframemanagerwithoutbatches")
    val tab = new Table
    tab.setTargetPartitionSize("30m")
    tab.setTargetVersion(1)

    manager.writeDataFrame(df, conn, tar, tab)

    val a = df.collect()
    val dep = Converters.convertTargetToDependency(tar)
    val loadedWholeDF = manager.loadDataFrame(conn, dep).sort("a", "b", "c")
    val collected = loadedWholeDF.collect()
    assert(a === collected)

    assert(
      new File("src/test/resources/testlake/testsparkframemanagerwithoutbatches/").isDirectory)
  }

  "save DF to Lake and load from lake" should "return the same df and create the right folderstructure" in {
    val spark = getSparkSession

    val df = createMockDataFrame(spark)
    val manager = new SparkFrameManager(spark)

    val conn = new Connection
    conn.setName("testSparkFrameManager")
    conn.setType("fs")
    conn.setStartPath("src/test/resources/testlake/")
    val tar = new Target
    tar.setConnectionName("testSparkFrameManager")
    tar.setPath("testsparkframemanager")
    val tab = new Table
    tab.setTargetPartitionSize("30m")
    tab.setTargetVersion(1)
    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)

    manager.writeDataFrame(df, conn, tar, tab, Option(dateTime))

    val dep = Converters.convertTargetToDependency(tar)
    val loadedDF = manager.loadDataFrame(conn, dep, Option(dateTime)).sort("a", "b", "c")

    val a = df.collect()
    val b = loadedDF.collect()
    assert(a === b)

    // loading the whole df without batch info
    val loadedWholeDF = manager.loadDataFrame(conn, dep).sort("a", "b", "c")
    val collected = loadedWholeDF.drop("date", "batch").collect()
    assert(loadedWholeDF.columns.contains("date") && loadedWholeDF.columns.contains("batch"))
    assert(a === collected)

    assert(
      new File("src/test/resources/testlake/testsparkframemanager/" +
        "date=2020-02-04/batch=2020-02-04-10-1").isDirectory)

    // loading should work also with a Source class
    val source = new Source
    source.setPartitionSize("30m")
    source.setConnectionName("testSparkFrameManager")
    source.setPath("testsparkframemanager")
    source.setVersion(1)
    val loadedDFSource = manager.loadDataFrame(conn, source, Option(dateTime)).sort("a", "b", "c")
    val bSource = loadedDF.collect()
    assert(a === bSource)
  }

  "load DF with aggregate dependencies" should "properly load the batches" in {
    val spark = getSparkSession

    val df = createMockDataFrame(spark)
    val manager = new SparkFrameManager(spark)

    val conn = new Connection
    conn.setName("testAggregateDependency")
    conn.setType("fs")
    conn.setStartPath("src/test/resources/testlake/")
    val tar = new Target
    tar.setConnectionName("testAggregateDependency")
    tar.setPath("testaggregatedependency")
    val tab = new Table
    tab.setTargetPartitionSize("15m")
    tab.setTargetVersion(1)

    List(2, 3, 4).foreach(day => List(10, 11).foreach(hour => List(0, 15, 30, 45).foreach(b => {
      val dateTime = LocalDateTime.of(2020, 7, day, hour, b)
      manager.writeDataFrame(df, conn, tar, tab, Option(dateTime))
      assert(new File("src/test/resources/testlake/testaggregatedependency/" +
        s"date=2020-07-0$day/batch=2020-07-0$day-$hour-${b % 15}").isDirectory)
    })))

    val dep = new Dependency
    dep.setPartitionSize("15m")
    dep.setType("aggregate")
    dep.setPartitionCount(4)
    dep.setConnectionName("testAggregateDependency")
    dep.setPath("testaggregatedependency")

    val depDateTime = LocalDateTime.of(2020, 7, 4, 10, 0)
    val loadedDF = manager.loadDataFrame(conn, dep, Option(depDateTime)).cache

    assert(loadedDF.count === df.count * 4)
    assert(loadedDF.select("batch").distinct().count === 4)

    // one day
    dep.setPartitionCount(4 * 24)
    val loadedDFDay = manager.loadDataFrame(conn, dep, Option(depDateTime)).cache
    assert(loadedDFDay.count === df.count * 4 * 2)
    assert(loadedDFDay.select("batch").distinct().count === 8)

    // one day written as 1d
    dep.setPartitionSize("1d")
    dep.setPartitionCount(1)
    val loadedDFDay2 = manager.loadDataFrame(conn, dep, Option(depDateTime)).cache
    assert(loadedDFDay2.count === df.count * 4 * 2)
    assert(loadedDFDay2.select("batch").distinct().count === 8)

    // one month
    dep.setPartitionSize("1d")
    dep.setPartitionCount(31)
    val loadedDFMonth = manager.loadDataFrame(conn, dep, Option(depDateTime)).cache
    assert(loadedDFMonth.count === df.count * 4 * 2 * 3)
    assert(loadedDFMonth.select("batch").distinct().count === 3 * 2 * 4)
  }

  "load DF with window dependencies" should "properly load the batches" in {
    val spark = getSparkSession

    val df = createMockDataFrame(spark)
    val manager = new SparkFrameManager(spark)

    val conn = new Connection
    conn.setName("testWindowDependency")
    conn.setType("fs")
    conn.setStartPath("src/test/resources/testlake/")
    val tar = new Target
    tar.setConnectionName("testWindowDependency")
    tar.setPath("testwindowdependency")
    val tab = new Table
    tab.setTargetPartitionSize("15m")
    tab.setTargetVersion(1)

    List(2, 3, 4).foreach(day => List(10, 11).foreach(hour => List(0, 15, 30, 45).foreach(b => {
      val dateTime = LocalDateTime.of(2020, 7, day, hour, b)
      manager.writeDataFrame(df, conn, tar, tab, Option(dateTime))
      assert(new File("src/test/resources/testlake/testwindowdependency/" +
        s"date=2020-07-0$day/batch=2020-07-0$day-$hour-${b % 15}").isDirectory)
    })))

    val dep = new Dependency
    dep.setPartitionSize("15m")
    dep.setType("window")
    dep.setPartitionCount(4)
    dep.setConnectionName("testWindowDependency")
    dep.setPath("testwindowdependency")

    val depDateTime = LocalDateTime.of(2020, 7, 4, 10, 0)
    val loadedDF = manager.loadDataFrame(conn, dep, Option(depDateTime)).cache

    assert(loadedDF.count === df.count)
    assert(loadedDF.select("batch").distinct().count === 1)

    val depDateTime2 = LocalDateTime.of(2020, 7, 4, 10, 30)
    val loadedDF2 = manager.loadDataFrame(conn, dep, Option(depDateTime2)).cache

    assert(loadedDF2.count === df.count * 3)
    assert(loadedDF2.select("batch").distinct().count === 3)

    val depDateTime3 = LocalDateTime.of(2020, 7, 4, 11, 15)
    val loadedDF3 = manager.loadDataFrame(conn, dep, Option(depDateTime3)).cache

    assert(loadedDF3.count === df.count * 4)
    assert(loadedDF3.select("batch").distinct().count === 4)

    // one day
    dep.setPartitionCount(4 * 24)
    val loadedDFDay = manager.loadDataFrame(conn, dep, Option(depDateTime3)).cache
    assert(loadedDFDay.count === df.count * 8)
    assert(loadedDFDay.select("batch").distinct().count === 8)

    // two days
    dep.setPartitionSize("1d")
    dep.setPartitionCount(2)
    val loadedDFDay2 = manager.loadDataFrame(conn, dep, Option(depDateTime3)).cache
    assert(loadedDFDay2.count === df.count * 16)
    assert(loadedDFDay2.select("batch").distinct().count === 16)

    // one month
    dep.setPartitionSize("1d")
    dep.setPartitionCount(31)
    val loadedDFMonth = manager.loadDataFrame(conn, dep, Option(depDateTime)).cache
    assert(loadedDFMonth.count === df.count * 4 * 2 * 3)
    assert(loadedDFMonth.select("batch").distinct().count === 3 * 2 * 4)
  }

  "load DFs" should "result in quick access to a range of batches" taggedAs Slow in {
    val spark = getSparkSession
    import spark.implicits._
    val manager = new SparkFrameManager(spark)
    val conn = new Connection
    conn.setName("testSparkRange")
    conn.setType("fs")
    conn.setStartPath("src/test/resources/testlake/")
    val tar = new Target
    tar.setConnectionName("testSparkRange")
    tar.setPath("testsparkframerange")
    val tab = new Table
    tab.setTargetPartitionSize("30m")
    tab.setTargetVersion(1)

    val tarDuration = tab.getTargetPartitionSizeDuration

    val batchDates = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2015, 10, 12, 0, 0),
      LocalDateTime.of(2015, 10, 15, 0, 0),
      tarDuration
    )
    batchDates.foreach(bd => {
      println(bd)
      val df = generateDF(spark, tarDuration, bd)
      manager.writeDataFrame(df, conn, tar, tab, Option(bd))
    })

    val dep = Converters.convertTargetToDependency(tar)
    val bigDF = manager.loadMoreBatches(
      conn,
      dep,
      LocalDateTime.of(2015, 10, 12, 20, 0),
      LocalDateTime.of(2015, 10, 13, 20, 0),
      tarDuration
    )
    val results = bigDF.agg(min($"batch"), max($"batch")).collect().head
    assert(results.getAs[String](0) === "2015-10-12-20-0")
    assert(results.getAs[String](1) === "2015-10-13-19-1")
  }

  "save DF to jdbc and load from jdbc without batches" should "return the same df and create the table in the db" taggedAs(Slow, DbTest) in {
    val spark = getSparkSession

    val df = createMockDataFrame(spark)
    val manager = new SparkFrameManager(spark)

    val conn = getJDBCConnection
    val tar = new Target
    tar.setConnectionName("testSparkJDBCFrameManager")
    tar.setPath(s"$DATABASE_NAME.testsparkframemanagerwithoutbatches")
    val tab = new Table
    tab.setTargetPartitionSize("30m")
    tab.setTargetVersion(1)
    tab.setTargetArea("staging")

    manager.writeDataFrame(df, conn, tar, tab)

    val dep = Converters.convertTargetToDependency(tar)
    val loadedDF = manager.loadDataFrame(conn, dep).sort("a", "b", "c")

    val a = df.collect()
    val b = loadedDF.collect()
    assert(a === b)
  }

  "save DF to jdbc and load from jdbc" should "return the same df and create the table in the db" taggedAs(Slow, DbTest) in {
    val spark = getSparkSession

    val df = createMockDataFrame(spark)
    val manager = new SparkFrameManager(spark)

    val conn = getJDBCConnection
    val tar = new Target
    tar.setConnectionName("testSparkJDBCFrameManager")
    tar.setPath(s"$DATABASE_NAME.testsparkframemanager")
    val tab = new Table
    tab.setTargetPartitionSize("30m")
    tab.setTargetVersion(1)
    tab.setTargetArea("staging")

    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)
    manager.writeDataFrame(df, conn, tar, tab, Option(dateTime))

    val dep = Converters.convertTargetToDependency(tar)
    val loadedDF = manager.loadDataFrame(conn, dep, Option(dateTime)).sort("a", "b", "c")

    val a = df.collect()
    val b = loadedDF.collect()
    assert(a === b)

    // loading the whole df without batch info
    val loadedWholeDF = manager.loadDataFrame(conn, dep).sort("a", "b", "c")
    val collected = loadedWholeDF.drop("date", "batch").collect()
    assert(loadedWholeDF.columns.contains("date") && loadedWholeDF.columns.contains("batch"))
    assert(a === collected)
  }
}
