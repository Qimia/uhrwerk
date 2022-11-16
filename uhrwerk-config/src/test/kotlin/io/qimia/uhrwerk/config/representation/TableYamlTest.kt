package io.qimia.uhrwerk.config.representation

import TestUtils.fileToString
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper
import org.junit.jupiter.api.Test


class TableYamlTest {


    @Test
    fun read() {
        val table = MAPPER.readValue<Table>(YAML)

        Truth.assertThat(table).isNotNull()
        Truth.assertThat(table.sources!!.toList()).hasSize(2)
        Truth.assertThat(table.dependencies!!.toList()).hasSize(5)
        Truth.assertThat(table.targets!!.toList()).hasSize(1)
        println(table)
    }

    companion object {
        private const val CONNECTION_YAML = "config/table1-config-new.yml"
        private val YAML = fileToString(CONNECTION_YAML)!!
        private val MAPPER = objectMapper()

    }
}