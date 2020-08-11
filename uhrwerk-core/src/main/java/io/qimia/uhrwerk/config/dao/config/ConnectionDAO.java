package io.qimia.uhrwerk.config.dao.config;

import io.qimia.uhrwerk.config.model.Connection;

import java.sql.SQLException;

public class ConnectionDAO {
    public static Long save(java.sql.Connection db, Connection connection)
            throws SQLException {
        io.qimia.uhrwerk.backend.model.config.Connection backendConnection = new io.qimia.uhrwerk.backend.model.config.Connection();
        backendConnection.setConnectionName(connection.getName());
        backendConnection.setConnectionType(connection.getType());
        backendConnection.setConnectionUrl(connection.getConnectionUrl());
        backendConnection.setVersion(String.valueOf(connection.getVersion()));
//        backendConnection.setDescription; todo missing

        return io.qimia.uhrwerk.backend.dao.config.ConnectionDAO.save(db, backendConnection);
    }
}
