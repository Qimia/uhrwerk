package io.qimia.uhrwerk.example.yelp.tools

import org.apache.spark.sql.SparkSession

object DownloadParquetTips extends App {


  val sparkSess = SparkSession.builder()
    .appName("downloader")
    .master("local")
    .getOrCreate()

  val jdbcDF = sparkSess.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:53306/yelp_db")
    .option("user", "root")
    .option("password", "mysql")
    .option("query", "SELECT * FROM tip WHERE date >= '2012-05-01' AND date < '2012-05-05'")
    .load()

  jdbcDF.write.parquet("./example_dataset/tip")

}
