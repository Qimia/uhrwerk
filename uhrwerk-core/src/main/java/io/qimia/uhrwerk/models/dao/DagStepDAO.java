package io.qimia.uhrwerk.models.dao;

import java.sql.*;

public class DagStepDAO {
  private static String INSERT_STM =
      "INSERT INTO "
          + "UHRWERK_METASTORE.DAG_STEP("
          + "table_spec_id, "
          + "parallelism, "
          + "max_partitions, "
          + "version, "
          + "description)"
          + "VALUES(?,?,?,?,?)";
  Connection db;

  public DagStepDAO(Connection db) {
    this.db = db;
  }

  public Long save(DagStep step) throws SQLException {
    PreparedStatement preparedStatement =
        db.prepareStatement(INSERT_STM, Statement.RETURN_GENERATED_KEYS);
    preparedStatement.setLong(1, step.getTableId());
    preparedStatement.setInt(2, step.getParallelism());
    preparedStatement.setInt(3, step.getMaxPartitions());
    preparedStatement.setString(4, step.getVersion());
    preparedStatement.setString(5, step.getDescription());
    preparedStatement.executeUpdate();
    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }
}
