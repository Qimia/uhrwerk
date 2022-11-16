package io.qimia.uhrwerk.config.convert

import TestUtils.filePath
import com.google.common.truth.Truth
import io.qimia.uhrwerk.config.builders.YamlConfigReader
import io.qimia.uhrwerk.config.representation.JDBC
import org.junit.jupiter.api.Test

class ConnectionMapTest {
    @Test
    fun read() {

        val connections = YamlConfigReader().readConnections(YAML_FILE)

        Truth.assertThat(connections).isNotNull()
        Truth.assertThat(connections!!.toList()).hasSize(3)

        connections?.forEach { println(it) }
    }

    companion object {
        private const val CONNECTION_YAML = "config/connection-config-new.yml"
        private val YAML_FILE = filePath(CONNECTION_YAML)!!

    }
}