package io.qimia.uhrwerk.engine

import org.apache.spark.sql.{DataFrame, SparkSession}

/** User's need to implement this trait for implementing a re-usable table function
  */
trait TableFunction {

  /** (Spark) Code for the function
    * @param inputs Map of DataFrames, all data needed for the function logic
    * @param args Map of AnyRef, all arguments needed for the function logic
    * @return DataFrame which is the result of the function
    */
  def process(
      inputs: Map[String, DataFrame],
      args: Map[String, AnyRef],
      spark: SparkSession
  ): DataFrame

}
