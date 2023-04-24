package io.qimia.uhrwerk.config.representation

import org.junit.jupiter.api.Test
import TestUtils.fileToString
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper

class FunctionCallTest{

    @Test
    fun read(){
        val funcCalls = objectMapper().readValue<Array<FunctionCall>>(YAML).toList()
        assertThat(funcCalls).isNotNull()
        assertThat(funcCalls).isNotEmpty()

        assertThat(funcCalls[0].name).isEqualTo("my_lookup_class_func")

        assertThat(funcCalls[0].args).isNotNull()
        assertThat(funcCalls[0].args).hasLength(2)


        assertThat(funcCalls[0].args?.get(0)?.name).isEqualTo("first_param")
        assertThat(funcCalls[0].args?.get(0)?.value).isEqualTo("first_param_value")

        assertThat(funcCalls[0].inputs).isNotNull()
        assertThat(funcCalls[0].inputs).hasLength(2)

        assertThat(funcCalls[0].inputs?.get(0)?.inputView).isEqualTo("table_a")
        assertThat(funcCalls[0].inputs?.get(0)?.tableView).isEqualTo("category_view")

    }

    companion object {
        private const val FUNCTION_DEFINITION_YAML = "config/function-calls.yml"
        private val YAML = fileToString(FUNCTION_DEFINITION_YAML)!!
    }
}