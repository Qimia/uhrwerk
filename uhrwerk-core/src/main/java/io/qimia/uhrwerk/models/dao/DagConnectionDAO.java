package io.qimia.uhrwerk.models.dao;

import io.qimia.uhrwerk.models.db.DagConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DagConnectionDAO {

  private static String INSERT_STM =
      "INSERT INTO "
          + "UHRWERK_METASTORE.DAG_CONNECTION("
          + "connection_name, "
          + "connection_type, "
          + "connection_url, "
          + "version, "
          + "description) VALUES "
          + "(?,?,?,?,?)";

  Connection db;

  public DagConnectionDAO(Connection db) {
    this.db = db;
  }

  public int save(DagConnection connection) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT_STM);
    insert.setString(1, connection.getConnectionName());
    insert.setString(2, connection.getConnectionType());
    insert.setString(3, connection.getConnectionUrl());
    insert.setString(4, connection.getVersion());
    insert.setString(5, connection.getDescription());
    return insert.executeUpdate();
  }
}
