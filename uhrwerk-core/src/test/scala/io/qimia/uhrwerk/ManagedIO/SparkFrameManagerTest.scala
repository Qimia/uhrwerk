package io.qimia.uhrwerk.ManagedIO

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration, LocalDateTime}
import java.util.Comparator

import io.qimia.uhrwerk.models.config.{Connection, Dependency, Target}
import io.qimia.uhrwerk.utils.{Converters, TimeTools}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterEach, Suite}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

trait BuildTeardown extends BeforeAndAfterEach { this: Suite =>

  val foldersToClean: ListBuffer[Path] = new ListBuffer[Path]

  override def afterEach() {
    try super.afterEach() // To be stackable, must call super.afterEach
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

  "when converting a LocalDateTime to a postfix it" should "be easily convertable" in {
    val dateTime = LocalDateTime.of(2020, 1, 1, 8, 45)
    val duration = Duration.ofMinutes(15)
    val outStr = SparkFrameManager.dateTimeToPostFix(dateTime, duration)
    val corrStr = s"2020-01-01-08-3"
    assert(outStr === corrStr)
  }

  "safe DF to Lake and load from lake" should "return the same df and create the right folderstructure" in {
    val spark = SparkSession
      .builder()
      .appName("TestFrameManager")
      .master("local")
      .getOrCreate()
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
    tar.setExternal(false)
    val dateTime = LocalDateTime.of(2020, 2, 4, 10, 30)
    manager.writeDFToLake(df, conn, tar, Option(dateTime))
    foldersToClean.append(Paths.get("src/test/resources/testlake/testsparkframemanager"))

    val dep = Converters.convertTargetToDependency(tar)
    val loadedDF = manager.loadDFFromLake(conn, dep, Option(dateTime))

    val a = df.collect()
    val b = loadedDF.collect()
    assert(a === b)

    assert(
      new File("src/test/resources/testlake/testsparkframemanager" +
        "/batch=2020-02-04-10-1").isDirectory)
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

  ignore should "result in quick access to a range of batches" in {
    val spark = SparkSession
      .builder()
      .appName("TestFrameManager2")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val manager = new SparkFrameManager(spark)
    val conn = new Connection
    conn.setName("testSparkFrameManager")
    conn.setType("fs")
    conn.setStartPath("src/test/resources/testlake/")
    val tar = new Target
    tar.setPartitionSize("30m")
    tar.setConnectionName("testSparkFrameManager")
    tar.setPath("testsparkframerange")
    tar.setVersion(1)
    tar.setExternal(false)
    val tarDuration = tar.getPartitionSizeDuration

    val batchDates = TimeTools.convertRangeToBatch(
      LocalDateTime.of(2015, 10, 12, 0, 0),
      LocalDateTime.of(2015, 10, 15, 0, 0),
      tarDuration
    )
    batchDates.foreach(bd => {
      val df = generateDF(spark, tarDuration, bd)
      manager.writeDFToLake(df, conn, tar, Option(bd))
    })
    foldersToClean.append(Paths.get("src/test/resources/testlake/testsparkframerange"))

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
    Thread.sleep(4000)  // Deleting the folders will else make the test fragile
  }

}
