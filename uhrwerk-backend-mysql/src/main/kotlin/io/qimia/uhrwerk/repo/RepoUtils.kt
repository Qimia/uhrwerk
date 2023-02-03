package io.qimia.uhrwerk.repo

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

object RepoUtils {

    private val mapper = jacksonObjectMapper()
    @JvmStatic
    fun toJson(obj: Any): String = mapper.writeValueAsString(obj)
    @JvmStatic
    fun jsonToArray(json: String): Array<String> = mapper.readValue(json)
    @JvmStatic
    fun jsonToMap(json: String): HashMap<String, Any> = mapper.readValue(json)

}