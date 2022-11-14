package io.qimia.uhrwerk

import org.junit.jupiter.api.Test
import java.sql.Timestamp
import java.time.LocalDateTime

class HashKeyTest {
    @Test
    fun hashNullField() {
        val res = java.lang.StringBuilder().append("Some-test-name").append(
            Timestamp.valueOf(
                LocalDateTime.now()
            )
        )
    }
}