package io.qimia.uhrwerk.config.representation

import TestUtils.fileToString
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper
import org.junit.jupiter.api.Test


class DependencyYamlTest {


    @Test
    fun read() {
        val dependencies = MAPPER.readValue<Array<Dependency>>(YAML).toList()

        Truth.assertThat(dependencies).isNotNull()
        Truth.assertThat(dependencies).hasSize(5)

        if (dependencies.isNotEmpty())
            dependencies.forEach { println(it) }

    }

    companion object {
        private const val CONNECTION_YAML = "config/dependency-config-new.yml"
        private val YAML = fileToString(CONNECTION_YAML)!!
        private val MAPPER = objectMapper()

    }
}