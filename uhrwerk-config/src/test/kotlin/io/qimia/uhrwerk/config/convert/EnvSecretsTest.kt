package io.qimia.uhrwerk.config.convert

import TestUtils.filePath
import com.google.common.truth.Truth
import io.qimia.uhrwerk.config.builders.YamlConfigReader
import org.junit.jupiter.api.Test

class EnvSecretsTest {
    @Test
    fun read() {
        val yamlFile = filePath("config/env-config-with-secrets.yml")!!
        val metastore = YamlConfigReader().readEnv(yamlFile)
        Truth.assertThat(metastore).isNotNull()
        Truth.assertThat(metastore.user).isNotEmpty()
        Truth.assertThat(metastore.pass).isNotEmpty()
        println(metastore)
    }

}