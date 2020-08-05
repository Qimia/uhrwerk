package io.qimia.uhrwerk.models.dao;

import io.qimia.uhrwerk.models.db.DagTableSpec;

import java.sql.*;

public class DagTableSpecDAO {
  private static String INSERT_STM =
      "INSERT INTO "
          + "UHRWERK_METASTORE.DAG_TABLE_SPEC("
          + "area, "
          + "vertical, "
          + "table_name, "
          + "version, "
          + "external, "
          + "description) "
          + "VALUES(?,?,?,?,?,?)";
  Connection db;

  public DagTableSpecDAO(Connection db) {
    this.db = db;
  }

  public Long save(DagTableSpec tableSpec) throws SQLException {
    PreparedStatement preparedStatement =
        db.prepareStatement(INSERT_STM, Statement.RETURN_GENERATED_KEYS);
    preparedStatement.setString(1, tableSpec.getArea());
    preparedStatement.setString(2, tableSpec.getVertical());
    preparedStatement.setString(3, tableSpec.getTableMame());
    preparedStatement.setString(4, tableSpec.getVersion());
    preparedStatement.setBoolean(5, tableSpec.getExternal());
    preparedStatement.setString(6, tableSpec.getDescription());
    preparedStatement.executeUpdate();
    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }
}
