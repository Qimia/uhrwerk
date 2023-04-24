package io.qimia.uhrwerk.config.representation

import org.junit.jupiter.api.Test
import TestUtils.fileToString
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper

class FunctionDefinitionTest{

    @Test
    fun read(){
        val funcDefs = objectMapper().readValue<Array<FunctionDefinition>>(YAML).toList()
        Truth.assertThat(funcDefs).isNotNull()
        Truth.assertThat(funcDefs).isNotEmpty()

    }

    companion object {
        private const val FUNCTION_DEFINITION_YAML = "config/function-definition.yml"
        private val YAML = fileToString(FUNCTION_DEFINITION_YAML)!!
        private val MAPPER = objectMapper()
    }
}