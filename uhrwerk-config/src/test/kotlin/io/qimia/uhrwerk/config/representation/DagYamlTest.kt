package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth
import TestUtils.fileToString
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper
import org.junit.jupiter.api.Test


class DagYamlTest {


    @Test
    fun read() {
        val dag = MAPPER.readValue<Dag>(YAML)

        Truth.assertThat(dag).isNotNull()
        Truth.assertThat(dag.connections!!.toList()).hasSize(3)
        Truth.assertThat(dag.connections!![0]).isInstanceOf(JDBC::class.java)

        Truth.assertThat(dag.tables!!.toList()).hasSize(2)

        Truth.assertThat(dag.tables!![0].sources).isNotNull()
        Truth.assertThat(dag.tables!![0].sources!!.toList()).hasSize(2)
        Truth.assertThat(dag.tables!![0].dependencies).isNull()
        Truth.assertThat(dag.tables!![0].targets).isNotNull()
        Truth.assertThat(dag.tables!![0].targets!!.toList()).hasSize(1)

        println(dag)
    }

    companion object {
        private const val CONNECTION_YAML = "config/dag-config-new.yml"
        private val YAML = fileToString(CONNECTION_YAML)!!
        private val MAPPER = objectMapper()

    }
}