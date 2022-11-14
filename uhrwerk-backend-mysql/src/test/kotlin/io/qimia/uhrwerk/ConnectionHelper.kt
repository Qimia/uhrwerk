package io.qimia.uhrwerk

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

object ConnectionHelper {
    @Throws(SQLException::class)
    fun getConnecion(): Connection {
        return DriverManager.getConnection(
                "jdbc:mysql://localhost:53306/UHRWERK_METASTORE",
                "UHRWERK_USER",
                "Xq92vFqEKF7TB8H9"
        )
    }
}