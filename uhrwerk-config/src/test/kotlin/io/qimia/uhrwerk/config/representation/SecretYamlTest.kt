package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.truth.Truth
import TestUtils.fileToString
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper
import org.junit.jupiter.api.Test

class SecretYamlTest {


    @Test
    fun read() {

        val secrets = MAPPER.readValue<Array<Secret>>(YAML).toList()

        Truth.assertThat(secrets).isNotNull()
        Truth.assertThat(secrets).hasSize(4)
        Truth.assertThat(secrets!![0]).isInstanceOf(AWSSecret::class.java)


        secrets?.forEach { println(it) }

        val aws = AWSSecret()
        aws.name = "meta_store_db_user"
        aws.awsSecretName = "uhrwerk/meta_store/db_user"
        aws.awsRegion = "eu_west_1"

        Truth.assertThat(secrets).contains(aws)
    }

    companion object {
        private const val YAML_FILE = "config/secret-config-new.yml"
        private val YAML = fileToString(YAML_FILE)!!
        private val MAPPER = objectMapper()

    }
}