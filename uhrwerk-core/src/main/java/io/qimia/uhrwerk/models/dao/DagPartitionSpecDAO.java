package io.qimia.uhrwerk.models.dao;

import java.sql.*;

public class DagPartitionSpecDAO {
  private static String INSERT_STM =
      "INSERT INTO " +
              "UHRWERK_METASTORE.DAG_PARTITION_SPEC(" +
              "table_spec_id, " +
              "batch_temporal_unit, " +
              "batch_size, " +
              "version, " +
              "description)"
          + "VALUES(?,?,?,?,?,?,?)";

  public static Long save(Connection db, DagDependency dependency) throws SQLException {
    PreparedStatement preparedStatement =
        db.prepareStatement(INSERT_STM, Statement.RETURN_GENERATED_KEYS);
    preparedStatement.setLong(1, dependency.getTableId());
    preparedStatement.setLong(2, dependency.getStepId());
    preparedStatement.setString(3, dependency.getPartitionTransform().name());
    preparedStatement.setString(4, dependency.getBatchTemporalUnit().name());
    preparedStatement.setInt(5, dependency.getBatchSize());
    preparedStatement.setString(6, dependency.getVersion());
    preparedStatement.setString(7, dependency.getDescription());
    preparedStatement.executeUpdate();
    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }
}
