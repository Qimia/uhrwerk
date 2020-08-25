package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Connection;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface ConnectionService {
  public ConnectionResult save(Connection connection, boolean overwrite);

  public Connection getByName(java.sql.Connection db, String name) throws SQLException;

    public Connection getById(Long id) throws SQLException;

  public Connection[] getConnections(PreparedStatement select) throws SQLException;
}
