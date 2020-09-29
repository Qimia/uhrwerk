package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Connection;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface ConnectionService {
  ConnectionResult save(Connection connection, boolean overwrite);

  Connection getByName(String name) throws SQLException;

  Connection getById(Long id) throws SQLException;

  Connection[] getConnections(PreparedStatement select) throws SQLException;
}
