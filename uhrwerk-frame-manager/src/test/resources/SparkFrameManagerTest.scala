package io.qimia.uhrwerk.framemanager

import io.qimia.uhrwerk.common.framemanager.BulkDependencyResult
import io.qimia.uhrwerk.common.model._
import io.qimia.uhrwerk.common.tools.{JDBCTools, TimeTools}
import io.qimia.uhrwerk.framemanager.utils.SparkFrameManagerUtils
import io.qimia.uhrwerk.tags.{DbTest, Slow}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.io.File
import java.nio.file.{Path, Paths}
import java.time.LocalDateTime
import scala.collection.mutable.ListBuffer

trait BuildTeardown extends BeforeAndAfterAll {
  this: Suite =>

  val DATABASE_NAME: String = "staging_dbname"
  val DATABASE_NAME2: String = "source_testdb"
  val DATABASE_NAME3: String = "staging_testdb"

  protected val logger: Logger = Logger.getLogger(this.getClass)

  override def afterAll() {
    val foldersToClean: ListBuffer[Path] = new ListBuffer[Path]
    foldersToClean.append(Paths.get("test_temp_dir/testlake/"))

    val conn = getJDBCConnection
    JDBCTools.dropJDBCDatabase(conn, DATABASE_NAME)
    JDBCTools.dropJDBCDatabase(conn, DATABASE_NAME2)
    JDBCTools.dropJDBCDatabase(conn, DATABASE_NAME3)
    foldersToClean.foreach(p => {
      try {
        cleanFolder(p)
      } catch {
        case exception: Exception =>
          logger.error("Couldn't remove folder")
          logger.error(exception.getLocalizedMessage)
      }
    })
  }

  def getJDBCConnection: ConnectionModel = {
    ConnectionModel
      .builder()
      .name("testJDBCConnection")
      .`type`(ConnectionType.JDBC)
      .jdbcDriver("com.mysql.cj.jdbc.Driver")
      .jdbcUrl("jdbc:mysql://localhost:53306?connectionTimeZone=LOCAL")
      //conn.setJdbcUrl("jdbc:mysql://localhost:53306")
      .jdbcUser("root")
      .jdbcPass("61ePGqq20u9TZjbNhf0")
      .build()

  }

  def cleanFolder(location: Path): Unit = {
    FileUtils.deleteDirectory(location.toFile)
  }
}

class SparkFrameManagerTest extends AnyFlatSpec with BuildTeardown {
  def getSparkSession: SparkSession = {
    val numberOfCores = Runtime.getRuntime.availableProcessors

    val conf = new SparkConf
    conf.set("spark.sql.session.timeZone", "UTC")

    val spark = SparkSession
      .builder()
      //.config(conf)
      .appName("TestFrameManager2")
      .master(s"local[${numberOfCores - 1}]")
      .getOrCreate()

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

  val file = new File("test_temp_dir/testlake")
  if (file.exists()) FileUtils.deleteDirectory(file)
  val temp_test_dir = file.getCanonicalPath

  "save DF to Lake and load from lake" should "return the same df and create the right folderstructure" taggedAs Slow in {
    val spark = getSparkSession

    val df = createMockDataFrame(spark)
    val manager = new SparkFrameManager(spark)

    val connection = generateConnection("testSparkFrameManager")

    val target =
      TargetModel.builder().connection(connection).format("parquet").build()

    val table = TableModel
      .builder()
      .partitionUnit(PartitionUnit.MINUTES)
      .partitionSize(30)
      .version("1")
      .area("staging")
      .name("testsparkframemanager")
      .vertical("testdb")
      .targets(Array(target))
      .build()

    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)

    manager.writeDataFrame(df, table, Array(dateTime))

    val dependency = Converters.convertTargetToDependency(target, table)
    val partition = Partition.builder().build()
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
        temp_test_dir + "/area=staging/vertical=testdb/table=testsparkframemanager/version=1/format=parquet/" +
          "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10/minute=2020-02-04-10-30"
      ).isDirectory
    )

    // loading should work also with a Source class
    val source = SourceModel
      .builder()
      .partitionSize(30)
      .partitionUnit(PartitionUnit.MINUTES)
      .connection(connection)
      .format("parquet")
      .path(
        "area=staging/vertical=testdb/table=testsparkframemanager/version=1/format=parquet"
      )
      .build()
    val loadedDFSource = manager
      .loadSourceDataFrame(source)
      .drop("year", "month", "day", "hour", "minute")
      .sort("a", "b", "c")
    val bSource = loadedDFSource.collect()
    assert(a === bSource)
  }

  private def generateConnection(
      connName: String = "testSparkFrameManager",
      connType: ConnectionType = ConnectionType.FS,
      connPath: String = temp_test_dir
  ) = {
    ConnectionModel
      .builder()
      .name(connName)
      .`type`(connType)
      .path(connPath)
      .build()
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

    timestamps.map(TimeTools.convertTSToString(_)).foreach(println)

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

    val connection = generateConnection("testSparkFrameManager")

    val connectionJDBC = getJDBCConnection

    val targetParquet = TargetModel
      .builder()
      .connection(connection)
      .format("parquet")
      .build()

    val targetCsv = TargetModel
      .builder()
      .connection(connection)
      .format("csv")
      .build()

    val targetJDBC = TargetModel
      .builder()
      .connection(connectionJDBC)
      .format("jdbc")
      .build()

    val table = TableModel
      .builder()
      .partitionUnit(PartitionUnit.MINUTES)
      .partitionSize(15)
      .version("1")
      .area("source")
      .name("sourcenumberone")
      .vertical("testdb")
      .targets(Array(targetParquet, targetCsv, targetJDBC))
      .build()

    val dataFrameOptions =
      Map("header" -> "true", "delimiter" -> "?", "inferSchema" -> "true")

    dfWithTS.printSchema()

    manager.writeDataFrame(
      dfWithTS,
      table,
      Array(),
      dataFrameWriterOptions = Option(
        Array(Map[String, String](), dataFrameOptions, Map[String, String]())
      )
    )

    val source = SourceModel
      .builder()
      .connection(connection)
      .format("parquet")
      .partitionSize(15)
      .partitionUnit(PartitionUnit.MINUTES)
      .path(
        "area=source/vertical=testdb/table=sourcenumberone/version=1/format=parquet"
      )
      .selectColumn("created_at")
      .build()

    val dateTime = timestamps.head

    val loadedDFSourceWithTS = manager
      .loadSourceDataFrame(
        source,
        startTS = Option(dateTime),
        endTSExcl = Option(dateTime.plusMinutes(source.getPartitionSize))
      )
      .drop("created_at")
      .sort("a", "b", "c")
    assert(
      SparkFrameManagerUtils.containsTimeColumns(
        loadedDFSourceWithTS,
        source.getPartitionUnit
      )
    )
    val bSourceWithTS = loadedDFSourceWithTS
      .drop(SparkFrameManagerUtils.timeColumns: _*)
      .collect()
    val a = df.sort("a", "b", "c").collect()
    assert(bSourceWithTS === a)

    // test for csv too
    val sourceCsv = SourceModel
      .builder()
      .connection(connection)
      .format("csv")
      .partitionSize(15)
      .partitionUnit(PartitionUnit.MINUTES)
      .path(
        "area=source/vertical=testdb/table=sourcenumberone/version=1/format=csv"
      )
      .selectColumn("created_at")
      .build()

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
    val sourceJDBC = SourceModel
      .builder()
      .connection(connectionJDBC)
      .format("jdbc")
      .partitionSize(15)
      .partitionUnit(PartitionUnit.MINUTES)
      .selectColumn("created_at")
      .selectQuery(
        "select * from <path> where created_at >= '<lower_bound>' and created_at < '<upper_bound>'"
      )
      .path("`source_testdb`.`sourcenumberone_1`")
      .build()

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

    val bSourceWithTSJDBCWithoutOptions =
      loadedDFSourceWithTSJDBCWithoutOptions.collect()
    assert(bSourceWithTSJDBCWithoutOptions === a)

      // with a partition query
      .parallelLoadColumn("a")
      .parallelLoadNum(20)
      .parallelLoadQuery(
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
    val bSourceWithTSJDBCWithPartitioning =
      loadedDFSourceWithTSJDBCWithPartitioning.collect()
    assert(bSourceWithTSJDBCWithPartitioning === a)

      // reading the whole dataframe
      .parallelLoadColumn(null)
      .parallelLoadNum(0)
      .parallelLoadQuery(null)
      .selectColumn(null)
      .selectQuery(null)

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

    val source = SourceModel
      .builder()
      .partitioned(true)
      .build()

    assertThrows[IllegalArgumentException](
      manager
        .loadSourceDataFrame(source, startTS = Option(LocalDateTime.now()))
    )
  }

  "loading bulk dependencies" should "result in quick access to a range of batches" in {
    val spark = getSparkSession
    import spark.implicits._
    val manager = new SparkFrameManager(spark)

    val connection = generateConnection(connName = "testSparkRange")

    val target = TargetModel
      .builder()
      .connection(connection)
      .format("parquet")
      .build()
    val table = TableModel
      .builder()
      .area("staging")
      .name("testsparkrange")
      .vertical("db")
      .version("1")
      .targets(Array(target))
      .partitionUnit(PartitionUnit.MINUTES)
      .build()

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
      logger.info(bd)
      val df = createMockDataFrame(spark, Option(bd))
      manager.writeDataFrame(df, table, Array(bd))
    })

    val dependency = Converters.convertTargetToDependency(target, table)
    val partitions = batchDates.map(bd => {
      val partition = Partition
        .builder()
        .partitionTs(bd)
        .partitionUnit(PartitionUnit.MINUTES)
        .partitionSize(1)
        .build()

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
    val partitions5 = partitions
      .slice(0, 1)
      .map(p => {
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

    val dependencyResult4 =
      BulkDependencyResult(batchDates, dependency, connection, partitions4)

    val bigDF4: DataFrame = manager.loadDependencyDataFrame(dependencyResult4)

    val results4 = bigDF4.agg(min($"day"), max($"day")).collect().head
    assert(results4.getAs[String](0).equals("2015-10-12"))
    assert(results4.getAs[String](1) === "2015-10-15")
  }

  "loading dependencies with empty partitions" should "fail" in {
    val spark = getSparkSession
    val manager = new SparkFrameManager(spark)

    val partitions =
      Array(Partition.builder().build(), Partition.builder().build())
    val dependencyResult = BulkDependencyResult(null, null, null, partitions)

    assertThrows[Exception](manager.loadDependencyDataFrame(dependencyResult))
  }

  "writing and loading with options with time columns" should "save and read a df" in {
    val spark = getSparkSession

    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)
    val df = createMockDataFrame(spark, Option(dateTime))
    val manager = new SparkFrameManager(spark)

    val connection = generateConnection()

    val target = TargetModel
      .builder()
      .connection(connection)
      .format("csv").build()

    val connectionJDBC = getJDBCConnection
    val targetJDBC = TargetModel
      .builder()
      .connection(connectionJDBC)
      .format("jdbc")
      .build()

    val table = TableModel
      .builder()
      .partitionUnit(PartitionUnit.MINUTES)
      .partitionSize(30)
      .version("1")
      .area("staging")
      .name("testoptions")
      .vertical("testdb")
      .targets(Array(target, targetJDBC))
      .build()

    val dataFrameOptions =
      Map("header" -> "true", "delimiter" -> "?", "inferSchema" -> "true")
    manager.writeDataFrame(
      df,
      table,
      Array(dateTime),
      Option(Array(dataFrameOptions))
    )

    val dependency = Converters.convertTargetToDependency(target, table)
    val partition = Partition
      .builder()
      .partitionTs(LocalDateTime.of(2020, 2, 4, 10, 30))
      .partitioned(true)
      .partitionSize(30)
      .partitionUnit(PartitionUnit.MINUTES)
      .build()
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
        temp_test_dir + "/area=staging/vertical=testdb/table=testoptions/version=1/format=csv/" +
          "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10/minute=2020-02-04-10-30"
      ).isDirectory
    )

    // loading should work also with a Source class
    val source = SourceModel
      .builder()
      .partitionSize(30)
      .partitionUnit(PartitionUnit.MINUTES)
      .connection(connection)
      .format("csv")
      .path(
        "area=staging/vertical=testdb/table=testoptions/version=1/format=csv"
      )
      .build()
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
    val df =
      createMockDataFrame(spark, Option(dateTime)) //.drop("hour", "minute")

    val manager = new SparkFrameManager(spark)

    val connection = generateConnection()

    val target = TargetModel
      .builder()
      .connection(connection)
      .format("csv")
      .build()
    val table = TableModel
      .builder()
      .partitionUnit(PartitionUnit.DAYS)
      .partitionSize(1)
      .version("1")
      .area("staging")
      .name("testoptionsonlyday")
      .vertical("testdb")
      .targets(Array(target))
      .build()

    val dataFrameOptions =
      Map("header" -> "true", "delimiter" -> "?", "inferSchema" -> "true")
    manager.writeDataFrame(
      df,
      table,
      Array(dateTime),
      Option(Array(dataFrameOptions))
    )

    val dependency = Converters.convertTargetToDependency(target, table)
    val partition = Partition
      .builder()
      .partitionTs(LocalDateTime.of(2020, 2, 4, 10, 30))
      .partitionUnit(PartitionUnit.DAYS)
      .partitionSize(1).build()

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
      SparkFrameManagerUtils.containsTimeColumns(
        loadedDF,
        table.getPartitionUnit
      )
    )

    assert(
      new File(
        temp_test_dir + "/area=staging/vertical=testdb/table=testoptionsonlyday/version=1/format=csv/" +
          "year=2020/month=2020-02/day=2020-02-04"
      ).isDirectory
    )

    assert(
      new File(
        temp_test_dir + "/area=staging/vertical=testdb/table=testoptionsonlyday/version=1/format=csv/" +
          "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10"
      ).isDirectory
    )

    // loading should work also with a Source class
    val source = SourceModel
      .builder()
      .partitionSize(1)
      .partitionUnit(PartitionUnit.DAYS)
      .connection(connection)
      .format("csv")
      .path(
        "area=staging/vertical=testdb/table=testoptionsonlyday/version=1/format=csv"
      )
      .build()

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
    assert(
      SparkFrameManagerUtils.containsTimeColumns(
        loadedDFSource,
        source.getPartitionUnit
      )
    )
  }

  "save DF to jdbc and load from jdbc" should "return the same df and create the table in the db" taggedAs (Slow, DbTest) in {
    val spark = getSparkSession

    val df = createMockDataFrame(spark)
    val manager = new SparkFrameManager(spark)

    val connection = getJDBCConnection

    val target = TargetModel
      .builder()
      .connection(connection)
      .format("jdbc")
      .build()

    val table = TableModel
      .builder()
      .partitionUnit(PartitionUnit.MINUTES)
      .partitionSize(15)
      .version("1.0")
      .area("staging")
      .name("framemanagertabletest")
      .vertical("dbname")
      .targets(Array(target))
      .build()

    val dateTime = LocalDateTime.of(2020, 2, 1, 3, 45)

    val partition = Partition
      .builder()
      .partitionSize(15)
      .partitionUnit(PartitionUnit.MINUTES)
      .partitionTs(dateTime)
      .key()

    manager.writeDataFrame(df, table, partitionTS = Array(dateTime))

    val dependency = Converters.convertTargetToDependency(target, table)

    val dependencyResult = BulkDependencyResult(
      Array(dateTime),
      dependency,
      connection,
      Array(partition)
    )

    val loadedDF = manager.loadDependencyDataFrame(dependencyResult)

    SparkFrameManagerUtils.timeColumns.foreach(c =>
      assert(loadedDF.columns.contains(c))
    )
    val dropped = loadedDF
      .drop(SparkFrameManagerUtils.timeColumns: _*)
      .sort("a", "b", "c")

    val a = df.collect()
    val b = dropped.collect()
    assert(a === b)
  }

  "writeDataFrame" should "throw an exception when one of the targets is missing a connection" in {
    val target = TargetModel
      .builder()
      .format("jdbc")
      .build()
    val table = TableModel
      .builder()
      .partitionUnit(PartitionUnit.MINUTES)
      .partitionSize(15)
      .version("1.0")
      .area("staging")
      .name("framemanagertabletest")
      .vertical("dbname")
      .targets(Array(target))
      .build()

    val spark = getSparkSession
    val manager = new SparkFrameManager(spark)

    assertThrows[IllegalArgumentException](
      manager.writeDataFrame(null, table, null)
    )
  }

  "saving several partitions into aggregates" should "result into proper partitioning" taggedAs Slow in {
    val spark = getSparkSession
    val manager = new SparkFrameManager(spark)

    val hours = List(15, 16)
    val minutes = List(0, 15, 30, 45)

    val df: DataFrame = hours
      .map(h =>
        minutes.map(m => {
          logger.info(h + " - " + m)
          val dt = LocalDateTime.of(2020, 9, 7, h, m)
          createMockDataFrame(spark, Option(dt))
        })
      )
      .reduce((a, b) => a.union(b))
      .reduce((a, b) => a.union(b))

    df.select("year", "month", "day", "hour", "minute").distinct().show()

    val connection =
      generateConnection(connName = "testSparkFrameManagerAggregation")

    val target = TargetModel
      .builder()
      .connection(connection)
      .format("parquet")
      .build()

    val connectionJDBC = getJDBCConnection
    val targetJDBC = TargetModel
      .builder()
      .connection(connectionJDBC)
      .format("jdbc")
      .build()

    val table = TableModel
      .builder()
      .partitionUnit(PartitionUnit.MINUTES)
      .partitionSize(45)
      .version("1")
      .area("staging")
      .name("testaggregates")
      .vertical("testdb")
      .targets(Array(target, targetJDBC))
      .build()

    val aggTS = Array(
      LocalDateTime.of(2020, 9, 7, 15, 0),
      LocalDateTime.of(2020, 9, 7, 15, 45),
      LocalDateTime.of(2020, 9, 7, 16, 30)
    )

    manager.writeDataFrame(df, table, aggTS)

    assert(
      aggTS.forall(ts =>
        new File(
          temp_test_dir + "/area=staging/vertical=testdb/table=testaggregates/version=1/format=parquet/" +
            SparkFrameManagerUtils.createDatePath(ts)
        ).isDirectory
      )
    )

    val loadedDF = spark.read
      .parquet(
        temp_test_dir + "/area=staging/vertical=testdb/table=testaggregates/version=1/format=parquet/"
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

    val connection =
      generateConnection(connName = "testSparkFrameManagerAggregation")

    val target = TargetModel
      .builder()
      .connection(connection)
      .format("parquet")
      .build()

    val connectionJDBC = getJDBCConnection
    val targetJDBC = TargetModel
      .builder()
      .connection(connectionJDBC)
      .format("jdbc")
      .build()

    val table = TableModel
      .builder()
      .partitioned(false)
      .partitionUnit(PartitionUnit.MINUTES)
      .version("1")
      .area("staging")
      .name("testunpartitioned")
      .vertical("testdb")
      .targets(Array(target, targetJDBC))
      .build()

    manager.writeDataFrame(
      df,
      table,
      Array(dateTime)
    )

    val dependency = Converters.convertTargetToDependency(target, table)
    dependency.setTransformType(PartitionTransformType.NONE)
    val partition = Partition.builder().build()
    partition.setPartitionTs(dateTime)
    partition.setPartitioned(false)
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
        temp_test_dir + "/area=staging/vertical=testdb/table=testunpartitioned/version=1/format=parquet/" +
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
