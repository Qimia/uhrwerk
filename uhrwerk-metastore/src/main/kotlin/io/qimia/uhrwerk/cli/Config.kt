package io.qimia.uhrwerk.common.cli

import java.util.*

class Config {
    var envConfig = "env-config.yml"
    var connConfig = "connection-config.yml"
    var tableConfig = "table-config.yml"
    var dagConfig = "dag-config.yml"
    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || javaClass != o.javaClass) return false
        val config = o as Config
        return envConfig == config.envConfig && connConfig == config.connConfig && tableConfig == config.tableConfig && dagConfig == config.dagConfig
    }

    override fun hashCode(): Int {
        return Objects.hash(envConfig, connConfig, tableConfig, dagConfig)
    }

    override fun toString(): String {
        return "Config{" +
                "envConfig='" + envConfig + '\'' +
                ", connConfig='" + connConfig + '\'' +
                ", tableConfig='" + tableConfig + '\'' +
                ", dagConfig='" + dagConfig + '\'' +
                '}'
    }
}