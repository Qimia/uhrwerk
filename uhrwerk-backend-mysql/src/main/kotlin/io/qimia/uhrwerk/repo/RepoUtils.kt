package io.qimia.uhrwerk.repo

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset

object RepoUtils {

    private val mapper = jacksonObjectMapper()

    @JvmStatic
    fun toJson(obj: Any): String = mapper.writeValueAsString(obj)

    @JvmStatic
    fun jsonToArray(json: String): Array<String> = mapper.readValue(json)

    @JvmStatic
    fun jsonToMap(json: String): HashMap<String, Any> = mapper.readValue(json)


    @JvmStatic
    fun jsonToStringMap(json: String): HashMap<String, String> = mapper.readValue(json)

    @JvmStatic
    fun convertTSToUTCString(date: LocalDateTime): String =
        Timestamp
            .valueOf(
                date
                    .atZone(ZoneId.systemDefault())
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toLocalDateTime()
            )
            .toString()

}