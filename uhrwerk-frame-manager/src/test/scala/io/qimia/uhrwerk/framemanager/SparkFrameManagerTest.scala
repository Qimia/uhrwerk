package io.qimia.uhrwerk.framemanager

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.time.LocalDateTime
import java.util.Comparator

import io.qimia.uhrwerk.common.framemanager.BulkDependencyResult
import io.qimia.uhrwerk.common.model._
import io.qimia.uhrwerk.common.tools.{JDBCTools, TimeTools}
import io.qimia.uhrwerk.framemanager.utils.SparkFrameManagerUtils
import io.qimia.uhrwerk.tags.{DbTest, Slow}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ListBuffer

trait BuildTeardown extends BeforeAndAfterAll {
  this: Suite =>

  val DATABASE_NAME: String = "staging_dbname"
  val DATABASE_NAME2: String = "source_testdb"
  val DATABASE_NAME3: String = "staging_testdb"

  override def afterAll() {
    val foldersToClean: ListBuffer[Path] = new ListBuffer[Path]
    foldersToClean.append(Paths.get("src/test/resources/testlake/"))

    val conn = getJDBCConnection
    JDBCTools.dropJDBCDatabase(conn, DATABASE_NAME)
    JDBCTools.dropJDBCDatabase(conn, DATABASE_NAME2)
    JDBCTools.dropJDBCDatabase(conn, DATABASE_NAME3)
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
    conn.setJdbcPass("61ePGqq20u9TZjbNhf0")
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

  def createMockDataFrame(
                           spark: SparkSession,
                           dateTime: Option[LocalDateTime] = Option.empty
                         ): DataFrame = {
    import spark.implicits._
    if (dateTime.isDefined) {
      val (year, month, day, hour, minute) =
        SparkFrameManagerUtils.getTimeValues(dateTime.get)
      (1 to 100)
        .map(i => (i, "txt", i * 5, year, month, day, hour, minute))
        .toDF("a", "b", "c", "year", "month", "day", "hour", "minute")
    } else {
      (1 to 100).map(i => (i, "txt", i * 5)).toDF("a", "b", "c")
    }
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

    manager.writeDataFrame(df, table, Array(dateTime))

    val dependency = Converters.convertTargetToDependency(target, table)
    val partition = new Partition
    partition.setPartitionTs(dateTime)
    partition.setPartitionUnit(PartitionUnit.MINUTES)
    partition.setPartitionSize(1)
    val dependencyResult = BulkDependencyResult(
      Array(dateTime),
      dependency,
      connection,
      Array(partition)
    )
    val loadedDF = manager
      .loadDependencyDataFrame(dependencyResult)
      .drop("year", "month", "day", "hour", "minute")
      .sort("a", "b", "c")

    val a = df.collect()
    val b = loadedDF.collect()
    assert(b === a)

    assert(
      new File(
        "src/test/resources/testlake/area=staging/vertical=testdb/table=testsparkframemanager/version=1/format=parquet/" +
          "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10/minute=2020-02-04-10-30"
      ).isDirectory
    )

    // loading should work also with a Source class
    val source = new Source
    source.setPartitionSize(30)
    source.setPartitionUnit(PartitionUnit.MINUTES)
    source.setConnection(connection)
    source.setFormat("parquet")
    source.setPath(
      "area=staging/vertical=testdb/table=testsparkframemanager/version=1/format=parquet"
    )
    val loadedDFSource = manager
      .loadSourceDataFrame(source)
      .drop("year", "month", "day", "hour", "minute")
      .sort("a", "b", "c")
    val bSource = loadedDFSource.collect()
    assert(a === bSource)
  }

  "source loading with timestamps" should "return proper partitions" in {
    val spark = getSparkSession

    val manager = new SparkFrameManager(spark)
    val df = createMockDataFrame(spark)
    val timestamps = List(
      LocalDateTime.of(2020, 2, 4, 10, 30),
      LocalDateTime.of(2020, 2, 4, 10, 45),
      LocalDateTime.of(2020, 2, 4, 11, 0)
    )

    val dfWithTS: DataFrame = timestamps
      .map(ts => {
        df.withColumn(
          "created_at",
          to_timestamp(
            lit(TimeTools.convertTSToString(ts))
          )
        )
      })
      .reduce((one, two) => one.union(two))
      .cache()

    dfWithTS.show()

    // save df first with some partitions
    assert(dfWithTS.count === 300)

    val connection = new Connection
    connection.setName("testSparkFrameManager")
    connection.setType(ConnectionType.FS)
    connection.setPath("src/test/resources/testlake/")

    val connectionJDBC = getJDBCConnection

    val targetParquet = new Target
    targetParquet.setConnection(connection)
    targetParquet.setFormat("parquet")

    val targetCsv = new Target
    targetCsv.setConnection(connection)
    targetCsv.setFormat("csv")

    val targetJDBC = new Target
    targetJDBC.setConnection(connectionJDBC)
    targetJDBC.setFormat("jdbc")

    val table = new Table
    table.setPartitionUnit(PartitionUnit.MINUTES)
    table.setPartitionSize(15)
    table.setVersion("1")
    table.setArea("source")
    table.setName("sourcenumberone")
    table.setVertical("testdb")
    table.setTargets(Array(targetParquet, targetCsv, targetJDBC))

    val dataFrameOptions =
      Map("header" -> "true", "delimiter" -> "?", "inferSchema" -> "true")
    manager.writeDataFrame(
      dfWithTS,
      table,
      Array(),
      dataFrameWriterOptions = Option(
        Array(Map[String, String](), dataFrameOptions, Map[String, String]())
      )
    )

    val source = new Source
    source.setConnection(connection)
    source.setFormat("parquet")
    source.setPartitionSize(15)
    source.setPartitionUnit(PartitionUnit.MINUTES)
    source.setPath(
      "area=source/vertical=testdb/table=sourcenumberone/version=1/format=parquet"
    )
    source.setSelectColumn("created_at")

    val dateTime = timestamps.head
    val loadedDFSourceWithTS = manager
      .loadSourceDataFrame(
        source,
        startTS = Option(dateTime),
        endTSExcl = Option(dateTime.plusMinutes(source.getPartitionSize))
      )
      .drop("created_at")
      .sort("a", "b", "c")
    assert(SparkFrameManagerUtils.containsTimeColumns(loadedDFSourceWithTS, source.getPartitionUnit))
    val bSourceWithTS = loadedDFSourceWithTS.drop(SparkFrameManagerUtils.timeColumns: _*).collect()
    val a = df.sort("a", "b", "c").collect()
    assert(bSourceWithTS === a)

    // test for csv too
    val sourceCsv = new Source
    sourceCsv.setConnection(connection)
    sourceCsv.setFormat("csv")
    sourceCsv.setPartitionSize(15)
    sourceCsv.setPartitionUnit(PartitionUnit.MINUTES)
    sourceCsv.setPath(
      "area=source/vertical=testdb/table=sourcenumberone/version=1/format=csv"
    )
    sourceCsv.setSelectColumn("created_at")

    val loadedDFSourceWithTSCsv = manager
      .loadSourceDataFrame(
        sourceCsv,
        startTS = Option(dateTime),
        endTSExcl = Option(dateTime.plusMinutes(sourceCsv.getPartitionSize)),
        dataFrameReaderOptions = Option(dataFrameOptions)
      )
      .drop("created_at")
      .drop(SparkFrameManagerUtils.timeColumns: _*)
      .sort("a", "b", "c")
    val bSourceWithTSCsv = loadedDFSourceWithTSCsv.collect()
    assert(bSourceWithTSCsv === a)

    // test for only startTS
    val loadedDFSourceWithTSCsvStartTs = manager
      .loadSourceDataFrame(
        sourceCsv,
        startTS = Option(dateTime.plusMinutes(30)),
        dataFrameReaderOptions = Option(dataFrameOptions)
      )
      .drop("created_at")
      .drop(SparkFrameManagerUtils.timeColumns: _*)
      .sort("a", "b", "c")
    val bSourceWithTSCsvStartTs = loadedDFSourceWithTSCsvStartTs.collect()
    assert(bSourceWithTSCsvStartTs === a)

    // test for only endTS
    val loadedDFSourceWithTSCsvEndTs = manager
      .loadSourceDataFrame(
        sourceCsv,
        endTSExcl = Option(dateTime.plusMinutes(15)),
        dataFrameReaderOptions = Option(dataFrameOptions)
      )
      .drop("created_at")
      .drop(SparkFrameManagerUtils.timeColumns: _*)
      .sort("a", "b", "c")
    val bSourceWithTSCsvEndTs = loadedDFSourceWithTSCsvEndTs.collect()
    assert(bSourceWithTSCsvEndTs === a)

    // test for jdbc
    val sourceJDBC = new Source
    sourceJDBC.setConnection(connectionJDBC)
    sourceJDBC.setFormat("jdbc")
    sourceJDBC.setPartitionSize(15)
    sourceJDBC.setPartitionUnit(PartitionUnit.MINUTES)
    sourceJDBC.setSelectColumn("created_at")
    sourceJDBC.setSelectQuery(
      "select * from <path> where created_at >= '<lower_bound>' and created_at < '<upper_bound>'"
    )
    sourceJDBC.setPath("`source_testdb`.`sourcenumberone_1`")

    val loadedDFSourceWithTSJDBC = manager
      .loadSourceDataFrame(
        sourceJDBC,
        startTS = Option(dateTime),
        endTSExcl = Option(dateTime.plusMinutes(sourceJDBC.getPartitionSize)),
        dataFrameReaderOptions = Option(dataFrameOptions)
      )
      .drop("created_at")
      .drop(SparkFrameManagerUtils.timeColumns: _*)
      .sort("a", "b", "c")
    val bSourceWithTSJDBC = loadedDFSourceWithTSJDBC.collect()
    assert(bSourceWithTSJDBC === a)

    // without options
    val loadedDFSourceWithTSJDBCWithoutOptions = manager
      .loadSourceDataFrame(
        sourceJDBC,
        startTS = Option(dateTime),
        endTSExcl = Option(dateTime.plusMinutes(sourceJDBC.getPartitionSize))
      )
      .drop("created_at")
      .drop(SparkFrameManagerUtils.timeColumns: _*)
      .sort("a", "b", "c")
    val bSourceWithTSJDBCWithoutOptions = loadedDFSourceWithTSJDBCWithoutOptions.collect()
    assert(bSourceWithTSJDBCWithoutOptions === a)

    // with a partition query
    sourceJDBC.setParallelLoadColumn("a")
    sourceJDBC.setParallelLoadNum(20)
    sourceJDBC.setParallelLoadQuery(
      "select a from <path> where created_at >= '<lower_bound>' and created_at < '<upper_bound>'"
    )
    val loadedDFSourceWithTSJDBCWithPartitioning = manager
      .loadSourceDataFrame(
        sourceJDBC,
        startTS = Option(dateTime),
        endTSExcl = Option(dateTime.plusMinutes(sourceJDBC.getPartitionSize))
      )
      .drop("created_at")
      .drop(SparkFrameManagerUtils.timeColumns: _*)
      .sort("a", "b", "c")
    val bSourceWithTSJDBCWithPartitioning = loadedDFSourceWithTSJDBCWithPartitioning.collect()
    assert(bSourceWithTSJDBCWithPartitioning === a)

    // reading the whole dataframe
    sourceJDBC.setParallelLoadColumn(null)
    sourceJDBC.setParallelLoadNum(0)
    sourceJDBC.setParallelLoadQuery(null)
    sourceJDBC.setSelectColumn(null)
    sourceJDBC.setSelectQuery(null)

    val loadedWholeDF = manager
      .loadSourceDataFrame(
        sourceJDBC
      )
      .drop(SparkFrameManagerUtils.timeColumns: _*)
      .sort("a", "b", "c")
    val bWholeDF = loadedWholeDF.collect()
    val dfWithTSCollected = dfWithTS.sort("a", "b", "c").collect()
    assert(bWholeDF === dfWithTSCollected)

    // reading the whole dataframe with select query
    sourceJDBC.setSelectQuery("select * from <path>")

    val loadedWholeDFQuery = manager
      .loadSourceDataFrame(
        sourceJDBC
      )
      .drop(SparkFrameManagerUtils.timeColumns: _*)
      .sort("a", "b", "c")
    val bWholeDFQuery = loadedWholeDFQuery.collect()
    assert(bWholeDFQuery === dfWithTSCollected)
  }

  "specifying timestamps but not the source select column for partitioned sources" should "throw an exception" in {
    val spark = getSparkSession
    val manager = new SparkFrameManager(spark)

    val source = new Source
    source.setPartitioned(true)

    assertThrows[IllegalArgumentException](
      manager
        .loadSourceDataFrame(source, startTS = Option(LocalDateTime.now()))
    )
  }

  "loading bulk dependencies" should "result in quick access to a range of batches" in {
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
    table.setPartitionUnit(PartitionUnit.MINUTES)

    val batchDates = Array(
      LocalDateTime.of(2015, 10, 12, 20, 0),
      LocalDateTime.of(2015, 10, 15, 13, 45)
    )

    val extraTs = Array(
      LocalDateTime.of(2015, 10, 20, 20, 0),
      LocalDateTime.of(2015, 10, 12, 23, 0),
      LocalDateTime.of(2015, 10, 12, 2, 0),
      LocalDateTime.of(2015, 10, 14, 19, 0),
      LocalDateTime.of(2015, 10, 14, 20, 0)
    )

    (batchDates ++ extraTs).foreach(bd => {
      println(bd)
      val df = createMockDataFrame(spark, Option(bd))
      manager.writeDataFrame(df, table, Array(bd))
    })

    val dependency = Converters.convertTargetToDependency(target, table)
    val partitions = batchDates.map(bd => {
      val partition = new Partition
      partition.setPartitionTs(bd)
      partition.setPartitionUnit(PartitionUnit.MINUTES)
      partition.setPartitionSize(1)

      partition
    })

    val dependencyResult =
      BulkDependencyResult(batchDates, dependency, connection, partitions)

    val bigDF: DataFrame = manager.loadDependencyDataFrame(dependencyResult)

    val results = bigDF.agg(min($"minute"), max($"minute")).collect().head
    assert(results.getAs[String](0) === "2015-10-12-20-00")
    assert(results.getAs[String](1) === "2015-10-15-13-45")

    // should also work with bigger partitions

    val partitions2 = partitions.map(p => {
      p.setPartitionUnit(PartitionUnit.HOURS)

      p
    })

    val dependencyResult2 =
      BulkDependencyResult(batchDates, dependency, connection, partitions2)

    val bigDF2: DataFrame = manager.loadDependencyDataFrame(dependencyResult2)

    val results2 = bigDF2.agg(min($"minute"), max($"minute")).collect().head
    assert(results2.getAs[String](0) === "2015-10-12-20-00")
    assert(results2.getAs[String](1) === "2015-10-15-13-45")

    // DAYS
    val partitions3 = partitions.map(p => {
      p.setPartitionUnit(PartitionUnit.DAYS)

      p
    })

    val dependencyResult3 =
      BulkDependencyResult(batchDates, dependency, connection, partitions3)

    val bigDF3: DataFrame = manager.loadDependencyDataFrame(dependencyResult3)

    val results3 = bigDF3.agg(min($"minute"), max($"minute")).collect().head
    assert(results3.getAs[String](0) === "2015-10-12-20-00")
    assert(results3.getAs[String](1) === "2015-10-15-13-45")

    // DAYS with partitionSize 2
    val partitions5 = partitions.slice(0, 1).map(p => {
      p.setPartitionUnit(PartitionUnit.DAYS)
      p.setPartitionSize(2)
      p
    })

    val dependencyResult5 =
      BulkDependencyResult(batchDates, dependency, connection, partitions5)

    val bigDF5: DataFrame = manager.loadDependencyDataFrame(dependencyResult5)

    val results5 = bigDF5.agg(min($"minute"), max($"minute")).collect().head
    assert(results5.getAs[String](0) === "2015-10-12-20-00")
    assert(results5.getAs[String](1) === "2015-10-14-19-00")

    // WEEKS
    val partitions4 = partitions
      .slice(0, 1)
      .map(p => {
        p.setPartitionUnit(PartitionUnit.WEEKS)
        p.setPartitionSize(1)

        p
      })

    val dependencyResult4 = BulkDependencyResult(batchDates, dependency, connection, partitions4)

    val bigDF4: DataFrame = manager.loadDependencyDataFrame(dependencyResult4)

    val results4 = bigDF4.agg(min($"day"), max($"day")).collect().head
    assert(results4.getAs[String](0).equals("2015-10-12"))
    assert(results4.getAs[String](1) === "2015-10-15")
  }

  "loading dependencies with empty partitions" should "fail" in {
    val spark = getSparkSession
    val manager = new SparkFrameManager(spark)

    val partitions = Array(new Partition, new Partition)
    val dependencyResult = BulkDependencyResult(null, null, null, partitions)

    assertThrows[Exception](manager.loadDependencyDataFrame(dependencyResult))
  }

  "writing and loading with options with time columns" should "save and read a df" in {
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

    val connectionJDBC = getJDBCConnection
    val targetJDBC = new Target
    targetJDBC.setConnection(connectionJDBC)
    targetJDBC.setFormat("jdbc")

    val table = new Table
    table.setPartitionUnit(PartitionUnit.MINUTES)
    table.setPartitionSize(30)
    table.setVersion("1")
    table.setArea("staging")
    table.setName("testoptions")
    table.setVertical("testdb")
    table.setTargets(Array(target, targetJDBC))

    val dataFrameOptions =
      Map("header" -> "true", "delimiter" -> "?", "inferSchema" -> "true")
    manager.writeDataFrame(
      df,
      table,
      Array(dateTime),
      Option(Array(dataFrameOptions))
    )

    val dependency = Converters.convertTargetToDependency(target, table)
    val partition = new Partition
    partition.setPartitionTs(LocalDateTime.of(2020, 2, 4, 10, 30))
    val dependencyResult = BulkDependencyResult(
      Array(dateTime),
      dependency,
      connection,
      Array(partition)
    )
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
      new File(
        "src/test/resources/testlake/area=staging/vertical=testdb/table=testoptions/version=1/format=csv/" +
          "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10/minute=2020-02-04-10-30"
      ).isDirectory
    )

    // loading should work also with a Source class
    val source = new Source
    source.setPartitionSize(30)
    source.setPartitionUnit(PartitionUnit.MINUTES)
    source.setConnection(connection)
    source.setFormat("csv")
    source.setPath(
      "area=staging/vertical=testdb/table=testoptions/version=1/format=csv"
    )
    val loadedDFSource = manager.loadSourceDataFrame(
      source,
      dataFrameReaderOptions = Option(dataFrameOptions)
    )
    val bSource = loadedDFSource
      .withColumn("year", col("year").cast(StringType))
      .withColumn("day", col("day").cast(StringType))
      .sort("a", "b", "c")
      .collect()
    assert(a === bSource)
  }

  "writing and loading with options with year-month-day columns" should "save and read a df" in {
    val spark = getSparkSession

    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)
    val df = createMockDataFrame(spark, Option(dateTime)) //.drop("hour", "minute")

    val manager = new SparkFrameManager(spark)

    val connection = new Connection
    connection.setName("testSparkFrameManager")
    connection.setType(ConnectionType.FS)
    connection.setPath("src/test/resources/testlake/")
    val target = new Target
    target.setConnection(connection)
    target.setFormat("csv")
    val table = new Table
    table.setPartitionUnit(PartitionUnit.DAYS)
    table.setPartitionSize(1)
    table.setVersion("1")
    table.setArea("staging")
    table.setName("testoptionsonlyday")
    table.setVertical("testdb")
    table.setTargets(Array(target))

    val dataFrameOptions =
      Map("header" -> "true", "delimiter" -> "?", "inferSchema" -> "true")
    manager.writeDataFrame(
      df,
      table,
      Array(dateTime),
      Option(Array(dataFrameOptions))
    )

    val dependency = Converters.convertTargetToDependency(target, table)
    val partition = new Partition
    partition.setPartitionTs(LocalDateTime.of(2020, 2, 4, 10, 30))
    partition.setPartitionUnit(PartitionUnit.DAYS)
    partition.setPartitionSize(1)

    val dependencyResult = BulkDependencyResult(
      Array(dateTime),
      dependency,
      connection,
      Array(partition)
    )
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
    assert(SparkFrameManagerUtils.containsTimeColumns(loadedDF, table.getPartitionUnit))

    assert(
      new File(
        "src/test/resources/testlake/area=staging/vertical=testdb/table=testoptionsonlyday/version=1/format=csv/" +
          "year=2020/month=2020-02/day=2020-02-04"
      ).isDirectory
    )

    assert(
      new File(
        "src/test/resources/testlake/area=staging/vertical=testdb/table=testoptionsonlyday/version=1/format=csv/" +
          "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10"
      ).isDirectory
    )

    // loading should work also with a Source class
    val source = new Source
    source.setPartitionSize(1)
    source.setPartitionUnit(PartitionUnit.DAYS)
    source.setConnection(connection)
    source.setFormat("csv")
    source.setPath(
      "area=staging/vertical=testdb/table=testoptionsonlyday/version=1/format=csv"
    )
    val loadedDFSource = manager.loadSourceDataFrame(
      source,
      dataFrameReaderOptions = Option(dataFrameOptions)
    )
    val bSource = loadedDFSource
      .withColumn("year", col("year").cast(StringType))
      .withColumn("day", col("day").cast(StringType))
      .sort("a", "b", "c")
      .collect()
    assert(a === bSource)
    assert(SparkFrameManagerUtils.containsTimeColumns(loadedDFSource, source.getPartitionUnit))
  }

  "save DF to jdbc and load from jdbc" should "return the same df and create the table in the db" taggedAs(Slow, DbTest) in {
    val spark = getSparkSession

    val df = createMockDataFrame(spark)
    val manager = new SparkFrameManager(spark)

    val connection = getJDBCConnection
    val target = new Target
    target.setConnection(connection)
    target.setFormat("jdbc")
    val table = new Table
    table.setPartitionUnit(PartitionUnit.MINUTES)
    table.setPartitionSize(15)
    table.setVersion("1.0")
    table.setArea("staging")
    table.setName("framemanagertabletest")
    table.setVertical("dbname")
    table.setTargets(Array(target))
    val dateTime = LocalDateTime.of(2020, 2, 1, 3, 45)
    val partition = new Partition
    partition.setPartitionSize(15)
    partition.setPartitionUnit(PartitionUnit.MINUTES)
    partition.setPartitionTs(dateTime)
    partition.setKey()

    manager.writeDataFrame(df, table, partitionTS = Array(dateTime))

    val dependency = Converters.convertTargetToDependency(target, table)
    val dependencyResult = BulkDependencyResult(
      Array(dateTime),
      dependency,
      connection,
      Array(partition)
    )
    val loadedDF = manager.loadDependencyDataFrame(dependencyResult)

    SparkFrameManagerUtils.timeColumns.foreach(c => assert(loadedDF.columns.contains(c)))
    val dropped = loadedDF
      .drop(SparkFrameManagerUtils.timeColumns: _*)
      .sort("a", "b", "c")

    val a = df.collect()
    val b = dropped.collect()
    assert(a === b)
  }

  "writeDataFrame" should "throw an exception when one of the targets is missing a connection" in {
    val target = new Target
    target.setFormat("jdbc")
    val table = new Table
    table.setPartitionUnit(PartitionUnit.MINUTES)
    table.setPartitionSize(15)
    table.setVersion("1.0")
    table.setArea("staging")
    table.setName("framemanagertabletest")
    table.setVertical("dbname")
    table.setTargets(Array(target))

    val spark = getSparkSession
    val manager = new SparkFrameManager(spark)

    assertThrows[IllegalArgumentException](manager.writeDataFrame(null, table, null))
  }

  "saving several partitions into aggregates" should "result into proper partitioning" taggedAs Slow in {
    val spark = getSparkSession
    val manager = new SparkFrameManager(spark)

    val hours = List(15, 16)
    val minutes = List(0, 15, 30, 45)

    val df: DataFrame = hours
      .map(h =>
        minutes.map(m => {
          println(h + " - " + m)
          val dt = LocalDateTime.of(2020, 9, 7, h, m)
          createMockDataFrame(spark, Option(dt))
        })
      )
      .reduce((a, b) => a.union(b))
      .reduce((a, b) => a.union(b))

    df.select("year", "month", "day", "hour", "minute").distinct().show()

    val connection = new Connection
    connection.setName("testSparkFrameManagerAggregation")
    connection.setType(ConnectionType.FS)
    connection.setPath("src/test/resources/testlake/")
    val target = new Target
    target.setConnection(connection)
    target.setFormat("parquet")

    val connectionJDBC = getJDBCConnection
    val targetJDBC = new Target
    targetJDBC.setConnection(connectionJDBC)
    targetJDBC.setFormat("jdbc")

    val table = new Table
    table.setPartitionUnit(PartitionUnit.MINUTES)
    table.setPartitionSize(45)
    table.setVersion("1")
    table.setArea("staging")
    table.setName("testaggregates")
    table.setVertical("testdb")
    table.setTargets(Array(target, targetJDBC))

    val aggTS = Array(
      LocalDateTime.of(2020, 9, 7, 15, 0),
      LocalDateTime.of(2020, 9, 7, 15, 45),
      LocalDateTime.of(2020, 9, 7, 16, 30)
    )

    manager.writeDataFrame(df, table, aggTS)

    assert(
      aggTS.forall(ts =>
        new File(
          "src/test/resources/testlake/area=staging/vertical=testdb/table=testaggregates/version=1/format=parquet/" +
            SparkFrameManagerUtils.createDatePath(ts, PartitionUnit.MINUTES)
        ).isDirectory
      )
    )

    val loadedDF = spark.read
      .parquet(
        "src/test/resources/testlake/area=staging/vertical=testdb/table=testaggregates/version=1/format=parquet/"
      )
      .cache

    assert(loadedDF.count === df.count)
    assert(
      loadedDF
        .select("year", "month", "day", "hour", "minute")
        .except(df.select("year", "month", "day", "hour", "minute"))
        .count === 0
    )
  }

  "writing and loading unpartitioned data" should "save and read a df" in {
    val spark = getSparkSession

    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)
    val df = createMockDataFrame(spark)
    val manager = new SparkFrameManager(spark)

    val connection = new Connection
    connection.setName("testSparkFrameManagerUnpartitioned")
    connection.setType(ConnectionType.FS)
    connection.setPath("src/test/resources/testlake/")
    val target = new Target
    target.setConnection(connection)
    target.setFormat("parquet")

    val connectionJDBC = getJDBCConnection
    val targetJDBC = new Target
    targetJDBC.setConnection(connectionJDBC)
    targetJDBC.setFormat("jdbc")

    val table = new Table
    table.setPartitioned(false)
    table.setPartitionUnit(PartitionUnit.MINUTES)
    table.setVersion("1")
    table.setArea("staging")
    table.setName("testunpartitioned")
    table.setVertical("testdb")
    table.setTargets(Array(target, targetJDBC))

    manager.writeDataFrame(
      df,
      table,
      Array(dateTime)
    )

    val dependency = Converters.convertTargetToDependency(target, table)
    dependency.setTransformType(PartitionTransformType.NONE)
    val partition = new Partition
    partition.setPartitionTs(dateTime)
    val dependencyResult = BulkDependencyResult(
      Array(dateTime),
      dependency,
      connection,
      Array(partition)
    )
    val loadedDF = manager
      .loadDependencyDataFrame(dependencyResult)

    val a = df
      .collect()
    val b = loadedDF
      .sort("a", "b", "c")
      .collect()

    assert(b === a)

    assert(
      new File(
        "src/test/resources/testlake/area=staging/vertical=testdb/table=testunpartitioned/version=1/format=parquet/" +
          "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10/minute=2020-02-04-10-30"
      ).isDirectory
    )

    // loading should work also with jdbc
    val dependencyJDBC = Converters.convertTargetToDependency(targetJDBC, table)
    dependencyJDBC.setTransformType(PartitionTransformType.NONE)
    val dependencyResultJDBC = BulkDependencyResult(
      Array(dateTime),
      dependencyJDBC,
      connectionJDBC,
      Array(partition)
    )
    val loadedDFJDBC = manager
      .loadDependencyDataFrame(dependencyResultJDBC)

    val bJDBC = loadedDFJDBC
      .sort("a", "b", "c")
      .collect()

    assert(bJDBC === a)
  }
}
