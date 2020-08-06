package io.qimia.uhrwerk.models.dao;

import java.sql.*;

public class DagTargetDAO {
  private static String INSERT_STM =
      "INSERT INTO "
          + "UHRWERK_METASTORE.DAG_TARGET("
          + "step_id, "
          + "table_id, "
          + "version, "
          + "description)"
          + "VALUES(?,?,?,?)";

  public static Long save(Connection db, DagTarget target) throws SQLException {
    PreparedStatement preparedStatement =
        db.prepareStatement(INSERT_STM, Statement.RETURN_GENERATED_KEYS);
    preparedStatement.setLong(1, target.getTableId());
    preparedStatement.setLong(2, target.getStepId());
    preparedStatement.setString(3, target.getVersion());
    preparedStatement.setString(4, target.getDescription());
    preparedStatement.executeUpdate();
    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }
}
