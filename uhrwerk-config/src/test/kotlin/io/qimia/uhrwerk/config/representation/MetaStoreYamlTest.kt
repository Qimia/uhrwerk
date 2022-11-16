package io.qimia.uhrwerk.config.representation

import TestUtils.fileToString
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper
import org.junit.jupiter.api.Test


class MetaStoreYamlTest {


    @Test
    fun read() {
        val env = MAPPER.readValue<Env>(YAML)
        Truth.assertThat(env).isNotNull()
        Truth.assertThat(env.metastore).isNotNull()
        Truth.assertThat(env.metastore?.envName).isEqualTo("config_junit_env")
        println(env)

    }

    companion object {
        private const val CONNECTION_YAML = "config/env-config-new.yml"
        private val YAML = fileToString(CONNECTION_YAML)!!
        private val MAPPER = objectMapper()

    }
}