package io.qimia.uhrwerk.framemanager.utils

import java.time.LocalDateTime

import io.qimia.uhrwerk.common.model.{Dependency, PartitionUnit, Table}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class SparkFrameManagerUtilsTest extends AnyFlatSpec {
  def getSparkSession: SparkSession = {
    val numberOfCores = Runtime.getRuntime.availableProcessors
    val spark = SparkSession
      .builder()
      .appName("SparkFrameManagerUtilsTest")
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

  "concatenatePaths" should "properly concatenate paths" in {
    val a = "a"
    val b = "b"
    val c = "/c"
    val d = "d/"
    val e = "/e/"

    val result = SparkFrameManagerUtils.concatenatePaths(a, b, c, d, e)
    assert(result === "a/b/c/d/e")
  }

  "getFullLocation" should "properly concatenate paths" in {
    val a = "a/b/c/d"
    val e = "e/f/g/h"

    val result = SparkFrameManagerUtils.getFullLocation(a, e)
    assert(result === "a/b/c/d/e/f/g/h")
  }

  "concatenateDateParts" should "join two date parts" in {
    val month = "2"
    val day = "4"

    val result = SparkFrameManagerUtils.concatenateDateParts(month, day)
    assert(result === "2-4")
  }

  "createDatePath" should "create a date path from timestamp" in {
    val ts = LocalDateTime.of(2020, 2, 4, 10, 30)

    val datePath = SparkFrameManagerUtils.createDatePath(ts, PartitionUnit.MINUTES)

    assert(
      datePath === "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10/minute=2020-02-04-10-30"
    )

    val datePathHour = SparkFrameManagerUtils.createDatePath(ts, PartitionUnit.HOURS)

    assert(
      datePathHour === "year=2020/month=2020-02/day=2020-02-04/hour=2020-02-04-10"
    )

    val datePathDay = SparkFrameManagerUtils.createDatePath(ts, PartitionUnit.DAYS)

    assert(
      datePathDay === "year=2020/month=2020-02/day=2020-02-04"
    )
  }

  "getTablePath" should "create a table path" in {
    val table = new Table
    table.setVersion("1")
    table.setArea("staging")
    table.setName("testsparkframemanager")
    table.setVertical("testdb")

    val tablePath = SparkFrameManagerUtils.getTablePath(table, true, "parquet")
    assert(
      tablePath === "area=staging/vertical=testdb/table=testsparkframemanager/version=1/format=parquet"
    )

    val tablePathJDBC = SparkFrameManagerUtils.getTablePath(table, false, "jdbc")
    assert(tablePathJDBC === "`staging_testdb`.`testsparkframemanager_1`")
  }

  "getDependencyPath" should "create a dependency path" in {
    val dependency = new Dependency
    dependency.setVersion("1")
    dependency.setArea("staging")
    dependency.setTableName("testsparkframemanager")
    dependency.setVertical("testdb")
    dependency.setFormat("parquet")

    val tablePath = SparkFrameManagerUtils.getDependencyPath(dependency, true)
    assert(
      tablePath === "area=staging/vertical=testdb/table=testsparkframemanager/version=1/format=parquet"
    )

    val tablePathJDBC = SparkFrameManagerUtils.getDependencyPath(dependency, false)
    assert(tablePathJDBC === "`staging_testdb`.`testsparkframemanager_1`")
  }

  "containsTimeColumns" should "return true if all time columns are present" in {
    val spark = getSparkSession
    val ts = LocalDateTime.of(2015, 10, 12, 20, 0)
    val df = createMockDataFrame(spark, Option(ts))
    val manager = new SparkFrameManager(spark)

    assert(SparkFrameManagerUtils.containsTimeColumns(df, PartitionUnit.MINUTES) === true)
    assert(SparkFrameManagerUtils.containsTimeColumns(df.drop("hour"), PartitionUnit.MINUTES) === false)
    assert(SparkFrameManagerUtils.containsTimeColumns(df.drop("hour"), PartitionUnit.HOURS) === false)
    assert(
      SparkFrameManagerUtils
        .containsTimeColumns(df.withColumn("day", lit("03")), PartitionUnit.MINUTES) === false
    )
  }
}
