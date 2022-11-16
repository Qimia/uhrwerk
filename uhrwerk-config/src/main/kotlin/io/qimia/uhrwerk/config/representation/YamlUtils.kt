package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule

object YamlUtils {
    @JvmStatic
    fun objectMapper(): ObjectMapper {
        val kotlinModule = KotlinModule.Builder()
            .configure(KotlinFeature.StrictNullChecks, true)
            .build()

        return ObjectMapper(YAMLFactory())
            .registerModule(kotlinModule)

    }
}