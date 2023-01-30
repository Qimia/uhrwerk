package io.qimia.uhrwerk.repo

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

object RepoUtils {

    private val mapper = jacksonObjectMapper()
    fun toJson(obj: Any): String = mapper.writeValueAsString(obj)
    fun jsonToArray(json: String): Array<String> = mapper.readValue(json)
    fun jsonToMap(json: String): HashMap<String, Any> = mapper.readValue(json)

}