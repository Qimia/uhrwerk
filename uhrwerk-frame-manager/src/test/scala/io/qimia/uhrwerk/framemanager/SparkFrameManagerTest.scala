package io.qimia.uhrwerk.framemanager

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration, LocalDateTime}
import java.util.Comparator

import io.qimia.uhrwerk.common.framemanager.BulkDependencyResult
import io.qimia.uhrwerk.common.model._
import io.qimia.uhrwerk.utils.{JDBCTools, TimeTools}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

trait BuildTeardown extends BeforeAndAfterAll {
  this: Suite =>

  val DATABASE_NAME: String = "uhrwerk_spark_frame_manager_test"

  override def afterAll() {
    val foldersToClean: ListBuffer[Path] = new ListBuffer[Path]
    foldersToClean.append(Paths.get("src/test/resources/testlake/"))

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

  def getJDBCConnection: Connection = {
    val conn = new Connection
    conn.setName("testJDBCConnection")
    conn.setType(ConnectionType.JDBC)
    conn.setJdbcDriver("com.mysql.cj.jdbc.Driver")
    conn.setJdbcUrl("jdbc:mysql://localhost:53306")
    conn.setJdbcUser("root")
    conn.setJdbcPass("mysql")
    conn
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

  "save DF to Lake and load from lake" should "return the same df and create the right folderstructure" in {
    val spark = getSparkSession

    val df = createMockDataFrame(spark)
    val manager = new SparkFrameManager(spark)

    val connection = new Connection
    connection.setName("testSparkFrameManager")
    connection.setType(ConnectionType.FS)
    connection.setPath("src/test/resources/testlake/")
    val target = new Target
    target.setConnection(connection)
    target.setFormat("parquet")
    val table = new Table
    table.setPartitionUnit(PartitionUnit.MINUTES)
    table.setPartitionSize(30)
    table.setVersion("1")
    table.setArea("staging")
    table.setName("testsparkframemanager")
    table.setVertical("testdb")
    table.setTargets(Array(target))
    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)

    val dependency = Converters.convertTargetToDependency(target, table)
    manager.writeDataFrame(df, table, Option(dateTime))

    val partition = new Partition
    partition.setYear("2020")
    partition.setMonth("2")
    partition.setDay("4")
    partition.setHour("10")
    partition.setMinute("30")
    val dependencyResult = BulkDependencyResult(Array(dateTime), dependency, connection, Array(partition))
    val loadedDF = manager
      .loadDependencyDataFrame(dependencyResult)
      .drop("year", "month", "day", "hour", "minute")
      .sort("a", "b", "c")

    val a = df.collect()
    val b = loadedDF.collect()
    assert(b === a)

    assert(
      new File("src/test/resources/testlake/area=staging/vertical=testdb/table=testsparkframemanager/version=1/format=parquet/" +
        "year=2020/month=2020-2/day=2020-2-4/hour=2020-2-4-10/minute=2020-2-4-10-30").isDirectory)

    // loading should work also with a Source class
    val source = new Source
    source.setPartitionSize(30)
    source.setPartitionUnit(PartitionUnit.MINUTES)
    source.setConnection(connection)
    source.setFormat("parquet")
    source.setPath("area=staging/vertical=testdb/table=testsparkframemanager/version=1/format=parquet")
    val loadedDFSource = manager.loadSourceDataFrame(source)
      .drop("year", "month", "day", "hour", "minute")
      .sort("a", "b", "c")
    val bSource = loadedDFSource.collect()
    assert(a === bSource)
  }

  //  "load DF with aggregate dependencies" should "properly load the batches" in {
  //    val spark = getSparkSession
  //
  //    val df = createMockDataFrame(spark)
  //    val manager = new SparkFrameManager(spark)
  //
  //    val conn = new Connection
  //    conn.setName("testAggregateDependency")
  //    conn.setType("fs")
  //    conn.setJdbcUrl("src/test/resources/testlake/")
  //    val tar = new Target
  //    tar.setConnectionName("testAggregateDependency")
  //    tar.setFormat("testaggregatedependency")
  //    val tab = new Table
  //    tab.setPartitionSize("15m")
  //    tab.setVersion("1")
  //
  //    List(2, 3, 4).foreach(day => List(10, 11).foreach(hour => List(0, 15, 30, 45).foreach(b => {
  //      val dateTime = LocalDateTime.of(2020, 7, day, hour, b)
  //      manager.writeDataFrame(df, conn, tar, tab, Option(dateTime))
  //      assert(new File("src/test/resources/testlake/testaggregatedependency/" +
  //        s"date=2020-07-0$day/batch=2020-07-0$day-$hour-${b % 15}").isDirectory)
  //    })))
  //
  //    val dep = new Dependency
  //    dep.setPartitionSize("15m")
  //    dep.setType("aggregate")
  //    dep.setPartitionCount(4)
  //    dep.setConnectionName("testAggregateDependency")
  //    dep.setFormat("testaggregatedependency")
  //
  //    val depDateTime = LocalDateTime.of(2020, 7, 4, 10, 0)
  //    val loadedDF = manager.loadDependencyDataFrame(conn, dep, Option(depDateTime)).cache
  //
  //    assert(loadedDF.count === df.count * 4)
  //    assert(loadedDF.select("batch").distinct().count === 4)
  //
  //    // one day
  //    dep.setPartitionCount(4 * 24)
  //    val loadedDFDay = manager.loadDependencyDataFrame(conn, dep, Option(depDateTime)).cache
  //    assert(loadedDFDay.count === df.count * 4 * 2)
  //    assert(loadedDFDay.select("batch").distinct().count === 8)
  //
  //    // one day written as 1d
  //    dep.setPartitionSize("1d")
  //    dep.setPartitionCount(1)
  //    val loadedDFDay2 = manager.loadDependencyDataFrame(conn, dep, Option(depDateTime)).cache
  //    assert(loadedDFDay2.count === df.count * 4 * 2)
  //    assert(loadedDFDay2.select("batch").distinct().count === 8)
  //
  //    // one month
  //    dep.setPartitionSize("1d")
  //    dep.setPartitionCount(31)
  //    val loadedDFMonth = manager.loadDependencyDataFrame(conn, dep, Option(depDateTime)).cache
  //    assert(loadedDFMonth.count === df.count * 4 * 2 * 3)
  //    assert(loadedDFMonth.select("batch").distinct().count === 3 * 2 * 4)
  //  }
  //
  //  "load DF with window dependencies" should "properly load the batches" in {
  //    val spark = getSparkSession
  //
  //    val df = createMockDataFrame(spark)
  //    val manager = new SparkFrameManager(spark)
  //
  //    val conn = new Connection
  //    conn.setName("testWindowDependency")
  //    conn.setType("fs")
  //    conn.setJdbcUrl("src/test/resources/testlake/")
  //    val tar = new Target
  //    tar.setConnectionName("testWindowDependency")
  //    tar.setFormat("testwindowdependency")
  //    val tab = new Table
  //    tab.setPartitionSize("15m")
  //    tab.setVersion("1")
  //
  //    List(2, 3, 4).foreach(day => List(10, 11).foreach(hour => List(0, 15, 30, 45).foreach(b => {
  //      val dateTime = LocalDateTime.of(2020, 7, day, hour, b)
  //      manager.writeDataFrame(df, conn, tar, tab, Option(dateTime))
  //      assert(new File("src/test/resources/testlake/testwindowdependency/" +
  //        s"date=2020-07-0$day/batch=2020-07-0$day-$hour-${b % 15}").isDirectory)
  //    })))
  //
  //    val dep = new Dependency
  //    dep.setPartitionSize("15m")
  //    dep.setType("window")
  //    dep.setPartitionCount(4)
  //    dep.setConnectionName("testWindowDependency")
  //    dep.setFormat("testwindowdependency")
  //
  //    val depDateTime = LocalDateTime.of(2020, 7, 4, 10, 0)
  //    val loadedDF = manager.loadDependencyDataFrame(conn, dep, Option(depDateTime)).cache
  //
  //    assert(loadedDF.count === df.count)
  //    assert(loadedDF.select("batch").distinct().count === 1)
  //
  //    val depDateTime2 = LocalDateTime.of(2020, 7, 4, 10, 30)
  //    val loadedDF2 = manager.loadDependencyDataFrame(conn, dep, Option(depDateTime2)).cache
  //
  //    assert(loadedDF2.count === df.count * 3)
  //    assert(loadedDF2.select("batch").distinct().count === 3)
  //
  //    val depDateTime3 = LocalDateTime.of(2020, 7, 4, 11, 15)
  //    val loadedDF3 = manager.loadDependencyDataFrame(conn, dep, Option(depDateTime3)).cache
  //
  //    assert(loadedDF3.count === df.count * 4)
  //    assert(loadedDF3.select("batch").distinct().count === 4)
  //
  //    // one day
  //    dep.setPartitionCount(4 * 24)
  //    val loadedDFDay = manager.loadDependencyDataFrame(conn, dep, Option(depDateTime3)).cache
  //    assert(loadedDFDay.count === df.count * 8)
  //    assert(loadedDFDay.select("batch").distinct().count === 8)
  //
  //    // two days
  //    dep.setPartitionSize("1d")
  //    dep.setPartitionCount(2)
  //    val loadedDFDay2 = manager.loadDependencyDataFrame(conn, dep, Option(depDateTime3)).cache
  //    assert(loadedDFDay2.count === df.count * 16)
  //    assert(loadedDFDay2.select("batch").distinct().count === 16)
  //
  //    // one month
  //    dep.setPartitionSize("1d")
  //    dep.setPartitionCount(31)
  //    val loadedDFMonth = manager.loadDependencyDataFrame(conn, dep, Option(depDateTime)).cache
  //    assert(loadedDFMonth.count === df.count * 4 * 2 * 3)
  //    assert(loadedDFMonth.select("batch").distinct().count === 3 * 2 * 4)
  //  }
  //
  //  "load DFs" should "result in quick access to a range of batches" taggedAs Slow in {
  //    val spark = getSparkSession
  //    import spark.implicits._
  //    val manager = new SparkFrameManager(spark)
  //    val conn = new Connection
  //    conn.setName("testSparkRange")
  //    conn.setType("fs")
  //    conn.setJdbcUrl("src/test/resources/testlake/")
  //    val tar = new Target
  //    tar.setConnectionName("testSparkRange")
  //    tar.setFormat("testsparkframerange")
  //    val tab = new Table
  //    tab.setPartitionSize("30m")
  //    tab.setVersion("1")
  //
  //    val tarDuration = tab.getPartitionSizeDuration
  //
  //    val batchDates = TimeTools.convertRangeToBatch(
  //      LocalDateTime.of(2015, 10, 12, 0, 0),
  //      LocalDateTime.of(2015, 10, 15, 0, 0),
  //      tarDuration
  //    )
  //    batchDates.foreach(bd => {
  //      println(bd)
  //      val df = generateDF(spark, tarDuration, bd)
  //      manager.writeDataFrame(df, conn, tar, tab, Option(bd))
  //    })
  //
  //    val dep = Converters.convertTargetToDependency(tar, tab)
  //    val bigDF = manager.loadMoreBatches(
  //      conn,
  //      dep,
  //      LocalDateTime.of(2015, 10, 12, 20, 0),
  //      LocalDateTime.of(2015, 10, 13, 20, 0),
  //      tarDuration
  //    )
  //    val results = bigDF.agg(min($"batch"), max($"batch")).collect().head
  //    assert(results.getAs[String](0) === "2015-10-12-20-0")
  //    assert(results.getAs[String](1) === "2015-10-13-19-1")
  //  }
  //
  //  "save DF to jdbc and load from jdbc without batches" should "return the same df and create the table in the db" taggedAs(Slow, DbTest) in {
  //    val spark = getSparkSession
  //
  //    val df = createMockDataFrame(spark)
  //    val manager = new SparkFrameManager(spark)
  //
  //    val conn = JDBCToolsTest.getJDBCConnection
  //    val tar = new Target
  //    tar.setConnectionName("testJDBCConnection")
  //    tar.setFormat(s"$DATABASE_NAME.testsparkframemanagerwithoutbatches")
  //    val tab = new Table
  //    tab.setPartitionSize("30m")
  //    tab.setVersion("1")
  //    tab.setArea("staging")
  //
  //    manager.writeDataFrame(df, conn, tar, tab)
  //
  //    val dep = Converters.convertTargetToDependency(tar, tab)
  //    val loadedDF = manager.loadDependencyDataFrame(conn, dep).sort("a", "b", "c")
  //
  //    val a = df.collect()
  //    val b = loadedDF.collect()
  //    assert(a === b)
  //  }
  //
  //  "save DF to jdbc and load from jdbc" should "return the same df and create the table in the db" taggedAs(Slow, DbTest) in {
  //    val spark = getSparkSession
  //
  //    val df = createMockDataFrame(spark)
  //    val manager = new SparkFrameManager(spark)
  //
  //    val conn = JDBCToolsTest.getJDBCConnection
  //    val tar = new Target
  //    tar.setConnectionName("testJDBCConnection")
  //    tar.setFormat(s"$DATABASE_NAME.testsparkframemanager")
  //    val tab = new Table
  //    tab.setPartitionSize("30m")
  //    tab.setVersion("1")
  //    tab.setArea("staging")
  //
  //    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)
  //    manager.writeDataFrame(df, conn, tar, tab, Option(dateTime))
  //
  //    val dep = Converters.convertTargetToDependency(tar, tab)
  //    val loadedDF = manager.loadDependencyDataFrame(conn, dep, Option(dateTime)).sort("a", "b", "c")
  //
  //    val a = df.collect()
  //    val b = loadedDF.collect()
  //    assert(a === b)
  //
  //    // loading the whole df without batch info
  //    val loadedWholeDF = manager.loadDependencyDataFrame(conn, dep).sort("a", "b", "c")
  //    val collected = loadedWholeDF.drop("date", "batch").collect()
  //    assert(loadedWholeDF.columns.contains("date") && loadedWholeDF.columns.contains("batch"))
  //    assert(a === collected)
  //  }
  //
  //  "writing and reading csv with some options specified" should "properly write and read a dataframe" in {
  //    val spark = getSparkSession
  //
  //    val df = createMockDataFrame(spark)
  //    val manager = new SparkFrameManager(spark)
  //
  //    val conn = new Connection
  //    conn.setName("testCsvReading")
  //    conn.setType("fs")
  //    conn.setJdbcUrl("src/test/resources/testlake/")
  //  }
}
