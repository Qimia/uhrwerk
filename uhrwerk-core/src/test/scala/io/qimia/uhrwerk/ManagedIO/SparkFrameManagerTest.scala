package io.qimia.uhrwerk.ManagedIO

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration, LocalDateTime}
import java.util.Comparator

import io.qimia.uhrwerk.models.config.{Connection, Dependency, Target}
import io.qimia.uhrwerk.utils.Converters
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters._

class SparkFrameManagerTest extends AnyFlatSpec {

  def cleanFolder(location: Path): Unit = {
    Files
      .walk(location)
      .sorted(Comparator.reverseOrder())
      .iterator()
      .asScala
      .map(_.toFile)
      .foreach(f => f.delete())
  }

  "when converting a LocalDateTime to a postfix it" should "be easily convertable" in {
    val dateTime = LocalDateTime.of(2020, 1, 1, 8, 45)
    val duration = Duration.ofMinutes(15)
    val outStr = SparkFrameManager.dateTimeToPostFix(dateTime, duration)
    val corrStr = s"/year=2020/month=2020-1/day=2020-1-1/hour=8/batch=3"
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

    val dep = Converters.convertTargetToDependency(tar)
    val loadedDF = manager.loadDFFromLake(conn, dep, Option(dateTime))

    val a = df.collect()
    val b = loadedDF.collect()
    assert(a === b)

    assert(
      new File("src/test/resources/testlake/testsparkframemanager" +
        "/year=2020/month=2020-2/day=2020-2-4/hour=10/batch=1").isDirectory)

    cleanFolder(Paths.get("src/test/resources/testlake/testsparkframemanager"))
  }

}
