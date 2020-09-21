package io.qimia.uhrwerk.cli

import java.util
import java.util.concurrent.Callable

import picocli.CommandLine
import picocli.CommandLine.{Command, Option}

@Command(name="Uhrwerk", version=Array("Scala picocli 1.0"),
  mixinStandardHelpOptions = true, description = Array("Run Uhrwerk from Command Line"))
class CommandLineInterface extends Callable[Int]{

  @Option(names = Array("-g","--global"), paramLabel = "ENV_CONF",
    description = Array("Path to the Environment Configuration"))
  private val environmentConfig = ""

  @Option(names = Array("-c", "--cons"), paramLabel="CON_CONF",
    description = Array("Paths to the Connection Configuration(s)"))
  private val connectionConf = new util.ArrayList[String]

  @Option(names = Array("-t", "--tables"), paramLabel="TAB_CONF",
    description = Array("Paths to the Table Configuration(s)"))
  private val tableConf = new util.ArrayList[String]

  @Option(names = Array("-r", "--run"), paramLabel = "RUN_TAB",
    description = Array("Table to run as 'area.vertical.table.version'")
  )
  private val runTable = ""

  @Option(names = Array("-st", "--start"), paramLabel="STARTTS",
    description = Array("Start point for the execution"))
  private val startTime = ""

  @Option(names = Array("-et", "--end"), paramLabel = "ENDTS",
    description = Array("End point for the execution"))
  private val endTime = ""

  @Option(names = Array("-d", "--dag"), paramLabel = "DAGMD",
    description = Array(""))
  private val dagM = "n"

  @Option(names = Array("-p", "--parallel"), paramLabel = "PARALLEL",
    description = Array(""))
  private val parallelRun = 1

  @Option(names = Array("-o", "--ovw"), paramLabel = "OVERWR",
    description = Array(""))
  private val overwrite = "n"

  override def call(): Int = {

    0
  }
}

object CommandLineInterface{
  def main(args: Array[String]): Unit = {
    System.exit(new CommandLine(new CommandLineInterface()).execute(args: _*))
  }
}
