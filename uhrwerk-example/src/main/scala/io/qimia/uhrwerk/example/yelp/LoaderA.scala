package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import java.util.concurrent.Executors

import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import org.apache.spark.sql.DataFrame

import scala.concurrent.ExecutionContext


object LoaderA extends App {

  def loaderAFunc(in: TaskInput): DataFrame = {
    // The most basic userFunction simply returns the input dataframe
    in.inputFrames.values.head
  }

  // TODO: Needs framemanager
  val uhrwerkEnvironment = Environment.build("testing-env-config.yml" ,null)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("loader-A.yml", loaderAFunc)

  // Uncomment and test as soon as framemanager has been merged (test BulkDependencyResult conversions before that)
//  val runTimes = Array(LocalDateTime.of(2012, 5, 1, 0, 0))
//  val singleExecutor = Executors.newSingleThreadExecutor()
//  implicit val executorRunner = ExecutionContext.fromExecutor(singleExecutor)
//  wrapper.get.runTasks(runTimes)

}
