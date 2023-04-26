package io.qimia.uhrwerk.config.representation

import TestUtils.fileToString
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper
import org.junit.jupiter.api.Test

class FunctionCallTest{

    @Test
    fun read(){
        val funcCalls = objectMapper().readValue<Array<FunctionCall>>(YAML).toList()
        assertThat(funcCalls).isNotNull()
        assertThat(funcCalls).isNotEmpty()

        assertThat(funcCalls[0].name).isEqualTo("my_lookup_class_func2")

        assertThat(funcCalls[0].args).isNotNull()
        assertThat(funcCalls[0].args).hasSize(2)


        assertThat(funcCalls[0].args?.get("first_param")).isNotNull()
        assertThat(funcCalls[0].args?.get("first_param")).isEqualTo("first_param_value")

        assertThat(funcCalls[0].inputs).isNotNull()
        assertThat(funcCalls[0].inputs).hasSize(1)

        assertThat(funcCalls[0].inputs?.get("table_a")).isNotNull()
        assertThat(funcCalls[0].inputs?.get("table_a")).isEqualTo("category_view")

    }

    companion object {
        private const val FUNCTION_DEFINITION_YAML = "config/function-calls.yml"
        private val YAML = fileToString(FUNCTION_DEFINITION_YAML)!!
    }
}