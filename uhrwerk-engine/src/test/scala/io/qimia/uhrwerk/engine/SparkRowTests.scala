package io.qimia.uhrwerk.engine

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import scala.collection.JavaConverters._

class SparkRowTests extends AnyFlatSpec {
  "Spark Row to map" should "work" in {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkRowTests")
      .getOrCreate()

    import spark.implicits._
    val columns = Seq("language", "users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd).toDF(columns: _*)
    val mpList = df
      .collect()
      .map(row => row.getValuesMap[Any](columns))
      .toList

    assert(mpList.size == 3)
    println(mpList.asJava)

  }

}
