package io.qimia.uhrwerk.repo

import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.repo.RepoUtils.jsonToArray
import io.qimia.uhrwerk.repo.RepoUtils.jsonToMap
import io.qimia.uhrwerk.repo.RepoUtils.toJson

import org.junit.jupiter.api.Test

class RepoUtilsTest {
    private val mapJson = "{\"Key1\":\"Value1\",\"Key2\":\"Value2\",\"Key3\":3}"
    private val arrayJson = "[\"Key1\",\"Key2\",\"Key3\"]"

    private val map = mapOf("Key1" to "Value1", "Key2" to "Value2", "Key3" to 3)
    private val array = arrayOf("Key1", "Key2", "Key3")

    @Test
    fun toJsonTest() {
        val json1 = toJson(map)
        assertThat(json1).isEqualTo(mapJson)

        val json2 = toJson(array)
        assertThat(json2).isEqualTo(arrayJson)
    }

    @Test
    fun jsonToArryTest() {
        val array1 = jsonToArray(arrayJson)
        assertThat(array1).hasLength(3)
        assertThat(array1).isEqualTo(array)
    }

    @Test
    fun jsonToMapTest() {
        val map1 = jsonToMap(mapJson)
        assertThat(map1).hasSize(3)
        assertThat(map1).isEqualTo(map)
    }
}