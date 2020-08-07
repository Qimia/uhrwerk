package io.qimia.uhrwerk.ManagedIO

import java.time.LocalDateTime

import io.qimia.uhrwerk.config.model.{Connection, Target}
import io.qimia.uhrwerk.utils.Converters
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class InMemManagerTest extends AnyFlatSpec {

  "InMemManager when writing a table" should "be able to read said table" in {
    val spark = SparkSession
      .builder()
      .appName("TestFrameManager")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val manager = new InMemFrameManager

    val df = (1 to 200).map(i => (i, "abcde", i * 7)).toDF("x", "y", "z")
    val conn = new Connection
    conn.setName("testinmem")
    val tar = new Target
    tar.setPartitionSize("15m")
    tar.setConnectionName("testinmem")
    tar.setPath("someframeone")
    tar.setVersion(1)

    val dateTime = LocalDateTime.of(2010, 2, 4, 10, 30)
    manager.writeDataFrame(df, conn, tar, Option(dateTime))

    val dfUnpartitioned = (1000 to 1200).map(i => (i, "1234qwerty", i - 123)).toDF("x", "y", "z")
    val tarU = new Target
    tarU.setType("unpartitioned")  // TODO: See if target-types change or not (what to do with unpartitioned)
    tarU.setConnectionName("testinmem")
    tarU.setPath("otherframetwo")
    tarU.setVersion(1)

    manager.writeDataFrame(dfUnpartitioned, conn, tarU)

    val dep = Converters.convertTargetToDependency(tar)
    val loadedDF = manager.loadDataFrame(conn, dep, Option(dateTime))

    val a = df.collect()
    val b = loadedDF.collect()
    assert(a === b)

    val depU = Converters.convertTargetToDependency(tarU)
    val loadedDF2 = manager.loadDataFrame(conn, depU)
    val in = dfUnpartitioned.collect()
    val out = loadedDF2.collect()
    assert(in === out)
  }
}
