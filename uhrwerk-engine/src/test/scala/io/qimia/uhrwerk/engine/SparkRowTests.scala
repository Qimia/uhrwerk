package io.qimia.uhrwerk.engine

import io.qimia.uhrwerk.config.builders.YamlConfigReader
import io.qimia.uhrwerk.engine.dag.DagTaskBuilder
import io.qimia.uhrwerk.repo.RepoUtils
import org.apache.spark.sql.execution.debug._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{Await, Future}


class SparkRowTests extends AnyFlatSpec {

  "Spark Parallel (Mulit) Job same nave Temp View" should " have diff content" in {
    //val pool = Executors.newFixedThreadPool(2)
    // create the implicit ExecutionContext based on our thread pool
    //implicit val xc = ExecutionContext.fromExecutorService(pool)

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkParallelJobTest")
      .getOrCreate()

    val data1 =
      Seq(
        ("James1", "Sales", "NY", 90000, 34, 10000),
        ("Michael1", "Sales", "NY", 86000, 56, 20000),
        ("Raman1", "Finance", "CA", 99000, 40, 24000),
        ("Jen1", "Finance", "NY", 79000, 53, 15000),
        ("Jeff1", "Marketing", "CA", 80000, 25, 18000),
        ("Kumar1", "Marketing", "NY", 91000, 50, 21000)
      )

    val data2 =
      Seq(
        ("Robert", "Sales", "CA", 81000, 30, 23000),
        ("Maria", "Finance", "CA", 90000, 24, 23000),
        ("Scott", "Finance", "NY", 83000, 36, 19000),
        ("Jen", "Finance", "NY", 79000, 53, 15000),
        ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      )

    val columns =
      Seq("employee_name", "department", "state", "salary", "age", "bonus")

    val futures =List(data1, data2).map { data =>
      Future {
        val threadName = Thread.currentThread().getName
        val threadId = Thread.currentThread().getId
        println(s"####### Thread name=$threadName, id=$threadId ########")
        val session = spark.newSession()
        val rdd = session.sparkContext.parallelize(data)
        val df = session.createDataFrame(rdd).toDF(columns: _*)
        df.createOrReplaceTempView("data")
        session.sql("select * from data").show()
      }
    }
    Await.result(Future.sequence(futures), Duration(4, MINUTES))


  }

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

    val states = df
      .distinct()
      .groupBy("state")
      .count()
      .drop("count")
      .collect()
      .map(row => row.getValuesMap[Any](Seq("state")))
      .toList

    println(states)

  }

  "Spark single partition write and all partition read" should "work" in {
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
        ("Kumar", "Marketing", "NY", 91000, 50, 21000),
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

    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy("state")
      .json("test_dir_orig_parts/")

    val states = df
      .distinct()
      .groupBy("state")
      .count()
      .drop("count")
      .collect()
      .map(row => row.getValuesMap[Any](Seq("state")))
      .toList

    df.printSchema()

    states.foreach(map => {
      val state = map("state")
      df.filter(col("state") === state)
        .withColumn("state", lower(col("state")))
        .write
        .mode(SaveMode.Overwrite)
        .json(s"test_temp_dir/state=${state}/")
    })

    println(states)
    states.map(map => {
      val state = map("state")
      s"test_temp_dir/state=${state}/"
    })

    val dfJson = spark.read.json("test_temp_dir")

    val states2 = dfJson
      .distinct()
      .groupBy("state")
      .count()
      .drop("count")
      .collect()
      .map(row => row.getValuesMap[Any](Seq("state")))
      .toList

    println(states2)

    dfJson.printSchema()
    dfJson.show()

    val paths = states
      .map(map => {
        val state = map("state")
        s"test_temp_dir/state=${state}/"
      })

    spark.read.json(paths: _*).show()

  }

  "Spark Row to map" should "work" in {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkRowTests")
      .getOrCreate()
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

    val excludes = DagTaskBuilder.getExcludeTables(
      map.asScala
    )
    println(excludes)
  }

}
