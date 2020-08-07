package io.qimia.uhrwerk.backend.dao.config;

import io.qimia.uhrwerk.backend.model.config.Connection;

import java.sql.*;

public class ConnectionDAO {

  private static String INSERT =
      "INSERT INTO "
          + "CF_CONNECTION(connection_name, connection_type, connection_url, version, description) VALUES(?,?,?,?,?)";

  public static Long save(java.sql.Connection db, Connection connection)
      throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    insert.setString(1, connection.getConnectionName());
    insert.setString(2, connection.getConnectionType());
    insert.setString(3, connection.getConnectionUrl());
    insert.setString(4, connection.getVersion());
    insert.setString(5, connection.getDescription());
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  private static String SELECT_BY_ID =
      "SELECT id,connection_name,connection_type,connection_url,version,created_ts,updated_ts FROM CF_CONNECTION WHERE id = ?";

  public static Connection get(java.sql.Connection db, Long id) throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_BY_ID);
    select.setLong(1, id);

    ResultSet record = select.executeQuery();

    if (record.next()) {
      Connection res = new Connection();
      res.setId(record.getLong(1));
      res.setConnectionName(record.getString(2));
      res.setConnectionType(record.getString(3));
      res.setConnectionUrl(record.getString(4));
      res.setVersion(record.getString(5));
      res.setCreatedTs(record.getTimestamp(6));
      res.setUpdatedTs(record.getTimestamp(7));
      return res;
    }

    return null;
  }
}
