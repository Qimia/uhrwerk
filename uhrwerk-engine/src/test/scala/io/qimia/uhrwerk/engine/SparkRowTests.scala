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

  "Spark groupBy" should "work faster than dictinct" in {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkGroupByDistinctTests")
      .getOrCreate()

    val data =
      Seq(
        ("James", "Sales", "NY", 90000, 34, 10000),
        ("Michael", "Sales", "NY", 86000, 56, 20000),
        ("Robert", "Sales", "CA", 81000, 30, 23000),
        ("Maria", "Finance", "CA", 90000, 24, 23000),
        ("Raman", "Finance", "CA", 99000, 40, 24000),
        ("Scott", "Finance", "NY", 83000, 36, 19000),
        ("Jen", "Finance", "NY", 79000, 53, 15000),
        ("Jeff", "Marketing", "CA", 80000, 25, 18000),
        ("Kumar", "Marketing", "NY", 91000, 50, 21000)
      )

    val columns =
      Seq("employee_name", "department", "state", "salary", "age", "bonus")
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd).toDF(columns: _*)

    val states = df.groupBy("state").count().drop("count")
      .collect()
      .map(row => row.getValuesMap[Any](Seq("state")))
      .toList

    println(states)

  }

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
    df1.select($"language").distinct().collect()
    df1.cache()

    df1.write.json("test.json")

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
