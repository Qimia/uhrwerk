package io.qimia.uhrwerk.tools

import io.qimia.uhrwerk.config.builders.YamlConfigReader
import io.qimia.uhrwerk.dao.ConnectionDAO
import io.qimia.uhrwerk.dao.SecretDAO
import io.qimia.uhrwerk.dao.TableDAO
import io.qimia.uhrwerk.repo.HikariCPDataSource
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option

import java.util.concurrent.Callable
import kotlin.system.exitProcess

@Command(
    name = "uhrwerk_dag_deploy_cli",
    mixinStandardHelpOptions = true,
    version = ["uhrwerk_dag_deploy_cli 1.0"],
    description = ["Deploy DAG Tables and Connections to Metastore."]
)
class UhrwerkDagDeployCli : Callable<Int> {

    @Option(
        names = ["-g", "--global"],
        paramLabel = "ENV_CONF",
        description = ["Path to the Environment Configuration"],
        required = true
    )
    lateinit var env: String

    @Option(
        names = ["-c", "--cons"],
        paramLabel = "CON_CONF",
        description = ["Paths to the Connection Configuration(s)"],
        required = false
    )
    lateinit var connectionConfigs: ArrayList<String>

    @Option(
        names = ["-t", "--table"],
        paramLabel = "TAB_CONF",
        description = ["Paths to the Table Configuration"],
        required = false
    )
    lateinit var tableConfig: String


    override fun call(): Int {
        val configReader = YamlConfigReader()

        val envConnInfo = configReader.readEnv(env)
        HikariCPDataSource.initConfig(
            envConnInfo.jdbc_url, envConnInfo.user, envConnInfo.pass
        )

        val secretDAO = SecretDAO()
        val connectionDao = ConnectionDAO()
        val tableDAO = TableDAO()



        if (!connectionConfigs.isNullOrEmpty()) {
            connectionConfigs.forEach { connConfigLoc ->
                println("Reading Connection config-file: $connConfigLoc")
                val connections = configReader.readConnectionsSecrets(connConfigLoc)
                if (!connections.secrets.isNullOrEmpty()) connections.secrets!!.forEach {
                    secretDAO.save(
                        it, true
                    )
                }
                if (!connections.connections.isNullOrEmpty()) connections.connections!!.forEach {
                    connectionDao.save(
                        it, true
                    )
                }
            }
        } else {
            if (!tableConfig.isNullOrEmpty()) {
                val table = configReader.readTable(tableConfig)
                tableDAO.save(table, true)
            }
        }

        return 0
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>): Unit =
            exitProcess(CommandLine(UhrwerkDagDeployCli()).execute(*args))
    }
}

