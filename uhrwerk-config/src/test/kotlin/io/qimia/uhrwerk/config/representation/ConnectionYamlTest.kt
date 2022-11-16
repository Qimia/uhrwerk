package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth
import TestUtils.fileToString
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper
import org.junit.jupiter.api.Test

class ConnectionYamlTest {


    @Test
    fun read() {

        val connections = MAPPER.readValue<Array<Connection>>(YAML).toList()

        Truth.assertThat(connections).isNotNull()
        Truth.assertThat(connections).hasSize(3)
        Truth.assertThat(connections!![0]).isInstanceOf(JDBC::class.java)
        connections!![0].validate("/")


        connections?.forEach { println(it) }

        val jdbc = JDBC()
        jdbc.name = "mysql1"
        jdbc.jdbcUrl = "jdbc:mysql://localhost:3306"
        jdbc.jdbcDriver = "com.mysql.jdbc.Driver"
        jdbc.user = "root"
        jdbc.password = "mysql"

        Truth.assertThat(connections).contains(jdbc)
    }

    companion object {
        private const val CONNECTION_YAML = "config/connection-config-new.yml"
        private val YAML = fileToString(CONNECTION_YAML)!!
        private val MAPPER = objectMapper()

    }
}