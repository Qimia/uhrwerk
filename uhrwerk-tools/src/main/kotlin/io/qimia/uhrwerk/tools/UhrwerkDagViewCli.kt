package io.qimia.uhrwerk.tools

import io.qimia.uhrwerk.config.builders.YamlConfigReader
import io.qimia.uhrwerk.config.representation.Reference
import io.qimia.uhrwerk.repo.HikariCPDataSource
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option

import java.util.concurrent.Callable
import kotlin.system.exitProcess

@Command(
    name = "uhrwerk_tools_cli",
    mixinStandardHelpOptions = true,
    version = ["uhrwerk_tools_cli 1.0"],
    description = ["Display DAG Information for a Table."]
)
class UhrwerkDagViewCli : Callable<Int> {

    @Option(
        names = ["-g", "--global"],
        paramLabel = "ENV_CONF",
        description = ["Path to the Environment Configuration"],
        required = true
    )
    lateinit var env: String

    @Option(
        names = ["-t", "--table"],
        paramLabel = "TAB_CONF",
        description = ["Table in ref form"],
        required = true
    )
    lateinit var tableRef: String


    override fun call(): Int {
        val configReader = YamlConfigReader()

        val connInfo = configReader.readEnv(env)
        HikariCPDataSource.initConfig(
            connInfo.jdbc_url,
            connInfo.user,
            connInfo.pass
        )
        val tblRef = toTableRef(tableRef)
        println()
        println("######### DAG Tables Top Down ###########")
        val dag = UhrwerkDagBuilder.dag(tblRef)
        dag.forEach {
            println(it.toStringWithIndent())
        }

        println()
        println("######### DAG Tables in Processing Order ###########")
        val process = UhrwerkDagBuilder.process(tblRef)
        process.forEach { println(it.toStringWithIndent()) }

        return 0
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>): Unit =
            exitProcess(CommandLine(UhrwerkDagViewCli()).execute(*args))

        fun toTableRef(ref: String): Reference {
            var area: String? = null
            var vertical: String? = null
            var table: String? = null
            var version: String? = null

            if (!ref.isNullOrEmpty() && ref.isNotBlank()) {
                val groups = ref.split(":")
                val groupsNotEmpty = groups.all { it.isNotEmpty() }

                if (groupsNotEmpty && groups.size == 2) {
                    val names = groups[0].split(".")
                    val namesNotEmpty = names.all { it.isNotEmpty() }

                    if (namesNotEmpty && names.size == 3) {
                        area = names[0]
                        vertical = names[1]
                        table = names[2]
                    }
                    version = groups[1]
                }
            }
            return Reference(area, vertical, table, version)
        }

    }
}

