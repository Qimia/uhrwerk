package io.qimia.uhrwerk.config.representation

import TestUtils.fileToString
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper
import org.junit.jupiter.api.Test


class SourceYamlTest {


    @Test
    fun read() {
        val sources = MAPPER.readValue<Array<Source>>(YAML).toList()
        Truth.assertThat(sources).isNotNull()
        Truth.assertThat(sources).hasSize(2)
        if (sources.isNotEmpty())
            sources.forEach { println(it) }

    }

    companion object {
        private const val CONNECTION_YAML = "config/sources-config-new.yml"
        private val YAML = fileToString(CONNECTION_YAML)!!
        private val MAPPER = objectMapper()

    }
}