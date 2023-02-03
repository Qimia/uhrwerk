package io.qimia.uhrwerk.engine

import io.qimia.uhrwerk.config.builders.YamlConfigReader
import io.qimia.uhrwerk.repo.RepoUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.debug._
import org.apache.spark.sql.types._

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

  "Spark Empty DF cache and unpersist" should "work" in {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkEmptyDFTests")
      .getOrCreate()

    import spark.implicits._
    val columns = Seq("language", "users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd).toDF(columns: _*)
    val df1 = df.filter($"language" === "Go")
    df1.cache()

    assert(df1.isEmpty)
    df1.explain(true)
    df1.debug()
    df1.queryExecution.debug.codegen()
    df1.unpersist(blocking = false)

  }

  "Reading YAML props" should "work" in {
    val path = getClass
      .getResource("/properties/part_param_test__t_object_properties.yml")
      .getPath
    println(path)
    val map = new YamlConfigReader().readProperties(path)
    RepoUtils.toJson(map)
    println(RepoUtils.toJson(map))

  }

}
