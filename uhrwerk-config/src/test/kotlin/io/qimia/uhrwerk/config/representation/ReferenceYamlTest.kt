package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper
import org.junit.jupiter.api.Test


class ReferenceYamlTest {


    @Test
    fun read() {
        val yaml = "\"staging.sourcedb_1.tableOne:1.0\""
        val reference = MAPPER.readValue<Reference>(yaml)
        Truth.assertThat(reference).isNotNull()
        println(reference)

        val yaml1 = """
              area: staging
              vertical: sourcedb_1
              table: tableOne  # On DB
              format: "jdbc"
              version: "1.0"
        """.trimIndent()

        val reference1 = MAPPER.readValue<Reference>(yaml1)
        Truth.assertThat(reference1).isNotNull()
        println(reference1)

        Truth.assertThat(reference).isEqualTo(reference)


    }

    companion object {
        private val MAPPER = objectMapper()

    }
}