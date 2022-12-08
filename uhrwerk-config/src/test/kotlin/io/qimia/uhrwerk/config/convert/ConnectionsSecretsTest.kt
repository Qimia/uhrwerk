package io.qimia.uhrwerk.config.convert

import TestUtils.filePath
import com.google.common.truth.Truth
import io.qimia.uhrwerk.config.builders.YamlConfigReader
import org.junit.jupiter.api.Test

class ConnectionsSecretsTest {
    @Test
    fun readWithSecrets() {
        val yamlFile = filePath("config/connections-secrets-config-new.yml")!!
        val conns = YamlConfigReader().readConnectionsSecrets(yamlFile)
        Truth.assertThat(conns).isNotNull()
        Truth.assertThat(conns.secrets).isNotEmpty()
        Truth.assertThat(conns.connections).isNotEmpty()
        println(conns)
    }

    @Test
    fun readWithoutSecrets() {
        val yamlFile = filePath("config/connections-empty-secrets-config.yml")!!
        val conns = YamlConfigReader().readConnectionsSecrets(yamlFile)
        Truth.assertThat(conns).isNotNull()
        Truth.assertThat(conns.secrets).isNull()
        Truth.assertThat(conns.connections).isNotEmpty()
        println(conns)
    }

}