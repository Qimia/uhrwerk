package io.qimia.uhrwerk.config.representation

import TestUtils.fileToString
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper
import org.junit.jupiter.api.Test


class MetaStoreYamlTest {


    @Test
    fun envRead() {
        val yaml = fileToString("config/env-config-new.yml")!!
        val env = MAPPER.readValue<Env>(yaml)
        assertThat(env).isNotNull()
        assertThat(env.metastore).isNotNull()
        assertThat(env.metastore?.envName).isEqualTo("config_junit_env")
        println(env)
    }

    @Test
    fun envWithSecretsRead() {
        val yaml = fileToString("config/env-config-with-secrets.yml")!!
        val env = MAPPER.readValue<Env>(yaml)
        assertThat(env).isNotNull()

        assertThat(env.secrets).isNotNull()
        assertThat(env.secrets?.toList()).hasSize(2)

        assertThat(env.secrets?.mapNotNull { it.name }).containsExactly(
            "uhrwerk_db_usr",
            "uhrwerk_db_passwd"
        )
        env.secrets?.forEach { println(it) }

        assertThat(env.metastore).isNotNull()
        assertThat(env.metastore?.envName).isEqualTo("config_junit_env")
        println(env.metastore)

    }

    companion object {
        private val MAPPER = objectMapper()
    }
}