package io.qimia.uhrwerk.cli

import java.time.LocalDateTime
import java.util
import java.util.concurrent.Callable

import io.qimia.uhrwerk.engine.Environment.TableIdent
import picocli.CommandLine
import picocli.CommandLine.{Command, Option}
import io.qimia.uhrwerk.engine.UhrwerkAppRunner
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

@Command(name = "Uhrwerk", version = Array("Scala picocli 1.0"),
  mixinStandardHelpOptions = true, description = Array("Run Uhrwerk from Command Line"))
class CommandLineInterface extends Callable[Int] {

  @Option(names = Array("-g", "--global"), paramLabel = "ENV_CONF",
    description = Array("Path to the Environment Configuration"), required = false)
  private val environmentConfig = ""

  @Option(names = Array("-c", "--cons"), paramLabel = "CON_CONF",
    description = Array("Paths to the Connection Configuration(s)"), required = false)
  private val connectionConfAL = new util.ArrayList[String]

  @Option(names = Array("-t", "--tables"), paramLabel = "TAB_CONF",
    description = Array("Paths to the Table Configuration(s)"), required = false)
  private val tableConfAL = new util.ArrayList[String]

  @Option(names = Array("-r", "--run"), paramLabel = "RUN_TAB",
    description = Array("Table to run as 'area.vertical.table.version'"), required = true)
  private val runTable = ""

  @Option(names = Array("-st", "--start"), paramLabel = "STARTTS",
    description = Array("Start point for the execution"), required = true)
  private val startTime = ""

  @Option(names = Array("-et", "--end"), paramLabel = "ENDTS",
    description = Array("End point for the execution"), required = true)
  private val endTime = ""

  @Option(names = Array("-d", "--dag"), paramLabel = "DAGMD",
    description = Array(""), required = true)
  private val dagM = "n"

  @Option(names = Array("-p", "--parallel"), paramLabel = "PARALLEL",
    description = Array("Size of the threadpool for running the tasks"), required = false)
  private val parallelRun = 1

  @Option(names = Array("-o", "--ovw"), paramLabel = "OVERWR",
    description = Array("If entries should be updated if they already exist"), required = false)
  private val overw = "n"

  override def call(): Int = {
    val dagMode = dagM match {
      case "y" => true
      case _ => false
    }
    val overwrite = overw match {
      case "y" => true
      case _ => false
    }
    val sparkSess = SparkSession.builder().appName(runTable).master("local").getOrCreate()

    val connectionConf = connectionConfAL.asScala.toArray
    val tableConf = tableConfAL.asScala.toArray

    val components = runTable.split(".")
    val target = TableIdent(components(0), components(1), components(2), components(3))

    try {
      UhrwerkAppRunner.runFiles(sparkSess, environmentConfig, connectionConf,
        tableConf, target, startTime.asInstanceOf[LocalDateTime], endTime.asInstanceOf[LocalDateTime], dagMode, parallelRun, overwrite)
      0
    }
    catch {
      case e: Exception => 1
    }
  }
}

object CommandLineInterface{
  def main(args: Array[String]): Unit = {
    System.exit(new CommandLine(new CommandLineInterface()).execute(args: _*))
  }
}

