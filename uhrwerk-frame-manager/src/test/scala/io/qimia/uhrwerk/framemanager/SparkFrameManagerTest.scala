package io.qimia.uhrwerk.framemanager

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.time.LocalDateTime
import java.util.Comparator

import io.qimia.uhrwerk.common.framemanager.BulkDependencyResult
import io.qimia.uhrwerk.common.model._
import io.qimia.uhrwerk.tags.Slow
import io.qimia.uhrwerk.utils.JDBCTools
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
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

  def createMockDataFrame(spark: SparkSession, dateTime: Option[LocalDateTime] = Option.empty): DataFrame = {
    import spark.implicits._
    if (dateTime.isDefined) {
      val (year, month, day, hour, minute) = SparkFrameManager.getTimeValues(dateTime.get)
      (1 to 100).map(i => (i, "txt", i * 5, year, month, day, hour, minute)).toDF("a", "b", "c", "year", "month", "day", "hour", "minute")
    } else {
      (1 to 100).map(i => (i, "txt", i * 5)).toDF("a", "b", "c")
    }
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

  "getFullLocation" should "properly concatenate paths" in {
    val a = "a/b/c/d"
    val e = "e/f/g/h"

    val result = SparkFrameManager.getFullLocation(a, e)
    assert(result === "a/b/c/d/e/f/g/h")
  }

  "concatenateDateParts" should "join two date parts" in {
    val month = "2"
    val day = "4"

    val result = SparkFrameManager.concatenateDateParts(month, day)
    assert(result === "2-4")
  }

  "createDatePath" should "create a date path from timestamp" in {
    val ts = LocalDateTime.of(2020, 2, 4, 10, 30)

    val datePath = SparkFrameManager.createDatePath(ts)

    assert(datePath === "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10/minute=2020-02-04-10-30")
  }

  "getTablePath" should "create a table path" in {
    val table = new Table
    table.setVersion("1")
    table.setArea("staging")
    table.setName("testsparkframemanager")
    table.setVertical("testdb")

    val tablePath = SparkFrameManager.getTablePath(table, true, "parquet")
    assert(tablePath === "area=staging/vertical=testdb/table=testsparkframemanager/version=1/format=parquet")

    val tablePathJDBC = SparkFrameManager.getTablePath(table, false, "jdbc")
    assert(tablePathJDBC === "staging-testdb.testsparkframemanager-1")
  }

  "getDependencyPath" should "create a dependency path" in {
    val dependency = new Dependency
    dependency.setVersion("1")
    dependency.setArea("staging")
    dependency.setTableName("testsparkframemanager")
    dependency.setVertical("testdb")
    dependency.setFormat("parquet")

    val tablePath = SparkFrameManager.getDependencyPath(dependency, true)
    assert(tablePath === "area=staging/vertical=testdb/table=testsparkframemanager/version=1/format=parquet")

    val tablePathJDBC = SparkFrameManager.getDependencyPath(dependency, false)
    assert(tablePathJDBC === "staging-testdb.testsparkframemanager-1")
  }

  "save DF to Lake and load from lake" should "return the same df and create the right folderstructure" taggedAs Slow in {
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

    manager.writeDataFrame(df, table, Option(dateTime))

    val dependency = Converters.convertTargetToDependency(target, table)
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
        "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10/minute=2020-02-04-10-30").isDirectory)

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

  "containsTimeColumns" should "return true if all time columns are present" in {
    val spark = getSparkSession
    val ts = LocalDateTime.of(2015, 10, 12, 20, 0)
    val df = createMockDataFrame(spark, Option(ts))
    val manager = new SparkFrameManager(spark)

    assert(manager.containsTimeColumns(df) === true)
    assert(manager.containsTimeColumns(df.drop("hour")) === false)
    assert(manager.containsTimeColumns(df.withColumn("day", lit("03"))) === false)
  }

  "load DFs" should "result in quick access to a range of batches" in {
    val spark = getSparkSession
    import spark.implicits._
    val manager = new SparkFrameManager(spark)
    val connection = new Connection
    connection.setName("testSparkRange")
    connection.setType(ConnectionType.FS)
    connection.setPath("src/test/resources/testlake/")
    val target = new Target
    target.setConnection(connection)
    target.setFormat("parquet")
    val table = new Table
    table.setArea("staging")
    table.setName("testsparkrange")
    table.setVertical("db")
    table.setVersion("1")
    table.setTargets(Array(target))

    val batchDates = Array(
      LocalDateTime.of(2015, 10, 12, 20, 0),
      LocalDateTime.of(2015, 10, 15, 13, 45)
    )
    batchDates.foreach(bd => {
      println(bd)
      val df = createMockDataFrame(spark, Option(bd))
      manager.writeDataFrame(df, table, Option(bd))
    })

    val dependency = Converters.convertTargetToDependency(target, table)
    val partitions = batchDates.map(bd => {
      val partition = new Partition
      partition.setYear(bd.getYear.toString)
      partition.setMonth(bd.getMonthValue.toString)
      partition.setDay(bd.getDayOfMonth.toString)
      partition.setHour(bd.getHour.toString)
      partition.setMinute(bd.getMinute.toString)

      partition
    })

    val dependencyResult = BulkDependencyResult(batchDates, dependency, connection, partitions)

    val bigDF: DataFrame = manager.loadDependencyDataFrame(dependencyResult)

    val results = bigDF.agg(min($"minute"), max($"minute")).collect().head
    assert(results.getAs[String](0) === "2015-10-12-20-00")
    assert(results.getAs[String](1) === "2015-10-15-13-45")

    // should also work with bigger partitions

    val partitions2 = batchDates.map(bd => {
      val partition = new Partition
      partition.setYear(bd.getYear.toString)
      partition.setMonth(bd.getMonthValue.toString)
      partition.setDay(bd.getDayOfMonth.toString)
      partition.setHour(bd.getHour.toString)

      partition
    })

    val dependencyResult2 = BulkDependencyResult(batchDates, dependency, connection, partitions2)

    val bigDF2: DataFrame = manager.loadDependencyDataFrame(dependencyResult2)

    val results2 = bigDF2.agg(min($"minute"), max($"minute")).collect().head
    assert(results2.getAs[String](0) === "2015-10-12-20-00")
    assert(results2.getAs[String](1) === "2015-10-15-13-45")

    val partitions3 = batchDates.map(bd => {
      val partition = new Partition
      partition.setYear(bd.getYear.toString)
      partition.setMonth(bd.getMonthValue.toString)
      partition.setDay(bd.getDayOfMonth.toString)

      partition
    })

    val dependencyResult3 = BulkDependencyResult(batchDates, dependency, connection, partitions3)

    val bigDF3: DataFrame = manager.loadDependencyDataFrame(dependencyResult3)

    val results3 = bigDF3.agg(min($"minute"), max($"minute")).collect().head
    assert(results3.getAs[String](0) === "2015-10-12-20-00")
    assert(results3.getAs[String](1) === "2015-10-15-13-45")

    val partitions4 = batchDates.slice(0, 1).map(bd => {
      val partition = new Partition
      partition.setYear(bd.getYear.toString)
      partition.setMonth(bd.getMonthValue.toString)

      partition
    })

    val dependencyResult4 = BulkDependencyResult(batchDates, dependency, connection, partitions4)

    val bigDF4: DataFrame = manager.loadDependencyDataFrame(dependencyResult4)

    val results4 = bigDF4.agg(min($"minute"), max($"minute")).collect().head
    assert(results4.getAs[String](0) === "2015-10-12-20-00")
    assert(results4.getAs[String](1) === "2015-10-15-13-45")

    val partitions5 = batchDates.slice(0, 1).map(bd => {
      val partition = new Partition
      partition.setYear(bd.getYear.toString)

      partition
    })

    val dependencyResult5 = BulkDependencyResult(batchDates, dependency, connection, partitions5)

    val bigDF5: DataFrame = manager.loadDependencyDataFrame(dependencyResult5)

    val results5 = bigDF5.agg(min($"minute"), max($"minute")).collect().head
    assert(results5.getAs[String](0) === "2015-10-12-20-00")
    assert(results5.getAs[String](1) === "2015-10-15-13-45")
  }

  "loading dependencies with empty partitions" should "fail" in {
    val spark = getSparkSession
    val manager = new SparkFrameManager(spark)

    val partitions = Array(new Partition, new Partition)
    val dependencyResult = BulkDependencyResult(null, null, null, partitions)

    assertThrows[Exception](manager.loadDependencyDataFrame(dependencyResult))
  }

  "writing and loading with options with time columns" should "work" in {
    val spark = getSparkSession

    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)
    val df = createMockDataFrame(spark, Option(dateTime))
    val manager = new SparkFrameManager(spark)

    val connection = new Connection
    connection.setName("testSparkFrameManager")
    connection.setType(ConnectionType.FS)
    connection.setPath("src/test/resources/testlake/")
    val target = new Target
    target.setConnection(connection)
    target.setFormat("csv")
    val table = new Table
    table.setPartitionUnit(PartitionUnit.MINUTES)
    table.setPartitionSize(30)
    table.setVersion("1")
    table.setArea("staging")
    table.setName("testoptions")
    table.setVertical("testdb")
    table.setTargets(Array(target))

    val dataFrameOptions = Map("header" -> "true",
      "delimiter" -> "?",
      "inferSchema" -> "true")
    manager.writeDataFrame(df, table, Option(dateTime), Option(dataFrameOptions))

    val dependency = Converters.convertTargetToDependency(target, table)
    val partition = new Partition
    partition.setYear("2020")
    partition.setMonth("2")
    partition.setDay("04")
    partition.setHour("10")
    partition.setMinute("30")
    val dependencyResult = BulkDependencyResult(Array(dateTime), dependency, connection, Array(partition))
    val loadedDF = manager
      .loadDependencyDataFrame(dependencyResult, Option(dataFrameOptions))

    val a = df
      .collect()
    val b = loadedDF
      .withColumn("year", col("year").cast(StringType))
      .withColumn("day", col("day").cast(StringType))
      .sort("a", "b", "c")
      .collect()

    assert(b === a)

    assert(
      new File("src/test/resources/testlake/area=staging/vertical=testdb/table=testoptions/version=1/format=csv/" +
        "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10/minute=2020-02-04-10-30").isDirectory)

    // loading should work also with a Source class
    val source = new Source
    source.setPartitionSize(30)
    source.setPartitionUnit(PartitionUnit.MINUTES)
    source.setConnection(connection)
    source.setFormat("csv")
    source.setPath("area=staging/vertical=testdb/table=testoptions/version=1/format=csv")
    val loadedDFSource = manager.loadSourceDataFrame(source, dataFrameReaderOptions = Option(dataFrameOptions))
    val bSource = loadedDFSource
      .withColumn("year", col("year").cast(StringType))
      .withColumn("day", col("day").cast(StringType))
      .sort("a", "b", "c")
      .collect()
    assert(a === bSource)
  }
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
