package io.qimia.uhrwerk.config.representation

import com.google.common.truth.Truth
import io.qimia.uhrwerk.config.representation.YamlUtils.objectMapper
import org.junit.jupiter.api.Test


class PartitionMappingTest {
    val mapper = objectMapper()

    @Test
    fun testPartitionMapping() {
        val partJson1 = """
            {
                "column": "column1",
                "value": "value1"
            }
        """.trimIndent()

        val part1 = mapper.readValue(partJson1, PartitionMapping::class.java)
        Truth.assertThat(part1.column).isEqualTo("column1")
        Truth.assertThat(part1.value).isEqualTo("value1")

        val partJson2 = """
            {
                "column": "column2"
            }
        """.trimIndent()

        val part2 = mapper.readValue(partJson2, PartitionMapping::class.java)
        Truth.assertThat(part2.column).isEqualTo("column2")
        Truth.assertThat(part2.value).isNull()


        val partJsonArray = """
            [
                {
                    "column": "column1",
                    "value": "value1"
                },
                {
                    "column": "column2"
                }
            ]
        """.trimIndent()
        val partArray = mapper.readValue(partJsonArray, Array<PartitionMapping>::class.java)
        Truth.assertThat(partArray.size).isEqualTo(2)
        Truth.assertThat(partArray[0].column).isEqualTo("column1")
        Truth.assertThat(partArray[0].value).isEqualTo("value1")
        Truth.assertThat(partArray[1].column).isEqualTo("column2")
        Truth.assertThat(partArray[1].value).isNull()

        val partitionMapping = PartitionMapping()
        partitionMapping.column = "column"
        partitionMapping.value = "value"
    }
}
