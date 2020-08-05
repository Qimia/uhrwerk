package io.qimia.uhrwerk.models.dao;

import io.qimia.uhrwerk.models.db.DtTable;

import java.sql.*;

public class DtTableDAO {
  private static String INSERT_STM =
      "INSERT INTO "
          + "UHRWERK_METASTORE.DT_TABLE("
          + "table_spec_id, "
          + "connection_id, "
          + "path, "
          + "version, "
          + "external, "
          + "description) "
          + "VALUES(?,?,?,?,?,?)";
  Connection db;

  public DtTableDAO(Connection db) {
    this.db = db;
  }

  public Long save(DtTable table) throws SQLException {
    PreparedStatement preparedStatement =
        db.prepareStatement(INSERT_STM, Statement.RETURN_GENERATED_KEYS);
    preparedStatement.setLong(1, table.getTableSpecId());
    preparedStatement.setLong(2, table.getConnectionId());
    preparedStatement.setString(3, table.getPath());
    preparedStatement.setString(4, table.getVersion());
    preparedStatement.setBoolean(5, table.getExternal());
    preparedStatement.setString(6, table.getDescription());
    preparedStatement.executeUpdate();
    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }
}
