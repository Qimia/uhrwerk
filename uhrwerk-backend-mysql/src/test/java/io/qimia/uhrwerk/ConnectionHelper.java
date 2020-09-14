package io.qimia.uhrwerk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionHelper {
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:mysql://localhost:53306/UHRWERK_METASTORE_UNIT_TESTS",
                "UHRWERK_USER",
                "Xq92vFqEKF7TB8H9"
        );
    }
}
