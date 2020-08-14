//package io.qimia.uhrwerk.ManagedIO
//
//import java.time.LocalDateTime
//
//import io.qimia.uhrwerk.config.model.{Connection, Table, Target}
//import io.qimia.uhrwerk.utils.Converters
//import org.apache.spark.sql.SparkSession
//import org.scalatest.flatspec.AnyFlatSpec
//
//class InMemManagerTest extends AnyFlatSpec {
//
//  "InMemManager when writing a table" should "be able to read said table" in {
//    val spark = SparkSession
//      .builder()
//      .appName("TestFrameManager")
//      .master("local")
//      .getOrCreate()
//    import spark.implicits._
//    val manager = new InMemFrameManager
//
//    val df = (1 to 200).map(i => (i, "abcde", i * 7)).toDF("x", "y", "z")
//    val conn = new Connection
//    conn.setName("testinmem")
//    val tar = new Target
//    tar.setConnectionName("testinmem")
//    tar.setFormat("someframeone")
//    val table = new Table
//    table.setPartitionSize("1h")
//
//    val dateTime = LocalDateTime.of(2010, 2, 4, 10, 30)
//    manager.writeDataFrame(df, conn, tar, table, Option(dateTime))
//
//    val dfUnpartitioned = (1000 to 1200).map(i => (i, "1234qwerty", i - 123)).toDF("x", "y", "z")
//    val tarU = new Target
//    tarU.setConnectionName("testinmem")
//    tarU.setFormat("otherframetwo")
//
//    manager.writeDataFrame(dfUnpartitioned, conn, tarU, table)
//
//    val dep = Converters.convertTargetToDependency(tar, table)
//    val loadedDF = manager.loadDependencyDataFrame(conn, dep, Option(dateTime))
//
//    val a = df.collect()
//    val b = loadedDF.collect()
//    assert(a === b)
//
//    val depU = Converters.convertTargetToDependency(tarU, table)
//    val loadedDF2 = manager.loadDependencyDataFrame(conn, depU)
//    val in = dfUnpartitioned.collect()
//    val out = loadedDF2.collect()
//    assert(in === out)
//  }
//}
