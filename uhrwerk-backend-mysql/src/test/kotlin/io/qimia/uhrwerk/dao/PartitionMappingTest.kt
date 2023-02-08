package io.qimia.uhrwerk.dao

import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.repo.RepoUtils
import org.junit.jupiter.api.Test

class PartitionMappingTest {
    @Test
    fun mapPartitionMappings() {
        val partMappingsJson = "{\"Key1\":\"\$Value1\$\",\"Key2\":300}"
        val partMappings = RepoUtils.jsonToMap(partMappingsJson)

        val properties = mapOf("Value1" to 400)

        val mappings = TableDAO().mapPartitionMappings(partMappings, properties)

        assertThat(mappings).hasSize(2)
        assertThat(mappings["Key1"]).isSameInstanceAs(properties["Value1"])
        assertThat(mappings["Key1"]).isEqualTo(400)

        val mappingsJson = RepoUtils.toJson(mappings)
        println(mappingsJson)

    }

    @Test
    fun alternative() {
        val partMappingsJson = "{\"Key1\":\"\$Value1\$\",\"Key2\":300}"
        val partitionMappings = RepoUtils.jsonToMap(partMappingsJson)
        val properties = mapOf("Value1" to 400)
        var mappings = mapOf<String, Any>()

        if (!partitionMappings.isNullOrEmpty())
            mappings = partitionMappings.mapValues {
                val value = it.value
                var propValue: Any? = value
                if (value is String && value.startsWith("\$") && value.endsWith("\$")) {
                    val propName = value.substring(1, value.length - 1)
                    propValue = properties[propName]
                    if (propValue == null) {
                        throw IllegalArgumentException("Property $propName not found in properties")
                    }
                }
                propValue!!
            }

        assertThat(mappings).hasSize(2)
        assertThat(mappings["Key1"]).isSameInstanceAs(properties["Value1"])
        assertThat(mappings["Key1"]).isEqualTo(400)

        val mappingsJson = RepoUtils.toJson(mappings)
        println(mappingsJson)
    }
}