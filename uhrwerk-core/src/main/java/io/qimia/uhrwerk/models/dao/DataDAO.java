package io.qimia.uhrwerk.models.dao;

import io.qimia.uhrwerk.models.db.data.*;

import java.sql.*;

public class DataDAO {
  private static String INSERT_DT_TABLE =
      "INSERT INTO UHRWERK_METASTORE.DT_TABLE(cf_table_id, cf_connection_id, path, version, description) VALUES (?,?,?,?,?)";

  public Long save(Connection db, DataTable table) throws SQLException {
    PreparedStatement preparedStatement =
        db.prepareStatement(INSERT_DT_TABLE, Statement.RETURN_GENERATED_KEYS);
    preparedStatement.setLong(1, table.getCfTableId());
    preparedStatement.setLong(2, table.getCfConnectionId());
    preparedStatement.setString(3, table.getPath());
    preparedStatement.setString(4, table.getVersion());
    preparedStatement.setString(5, table.getDescription());
    preparedStatement.executeUpdate();
    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  private static String INSERT_DT_DEPENDENCY =
      "INSERT INTO UHRWERK_METASTORE.DT_DEPENDENCY(cf_table_id, dt_table_id, partition_transform, batch_temporal_unit, batch_size, version, description) VALUES (?,?,?,?,?,?,?)";

  public static Long save(Connection db, DataDependency dependency) throws SQLException {
    PreparedStatement preparedStatement =
        db.prepareStatement(INSERT_DT_DEPENDENCY, Statement.RETURN_GENERATED_KEYS);
    preparedStatement.setLong(1, dependency.getCfTableId());
    preparedStatement.setLong(2, dependency.getDtTableId());
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

  private static String INSERT_DT_PARTITION =
      "INSERT INTO UHRWERK_METASTORE.DT_PARTITION(dt_table_id, path, year, month, day, hour, minute, partition_hash) VALUES (?,?,?,?,?,?,?,?)";

  public static Long save(Connection db, DataPartition partition) throws SQLException {
    PreparedStatement preparedStatement =
        db.prepareStatement(INSERT_DT_PARTITION, Statement.RETURN_GENERATED_KEYS);
    preparedStatement.setLong(1, partition.getDtTableId());
    preparedStatement.setString(2, partition.getPath());
    preparedStatement.setString(3, partition.getYear());
    preparedStatement.setString(4, partition.getMonth());
    preparedStatement.setString(5, partition.getDay());
    preparedStatement.setString(6, partition.getHour());
    preparedStatement.setString(7, partition.getMinute());
    preparedStatement.setString(8, partition.getPartitionHash());
    preparedStatement.executeUpdate();
    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  private static String INSERT_DT_PARTITION_DEPENDENCY =
      "INSERT INTO UHRWERK_METASTORE.DT_PARTITION_DEPENDENCY(partition_id, dependency_partition_id) VALUES (?,?)";

  public static Long save(Connection db, DataPartitionDependency partitionDependency)
      throws SQLException {
    PreparedStatement preparedStatement =
        db.prepareStatement(INSERT_DT_PARTITION_DEPENDENCY, Statement.RETURN_GENERATED_KEYS);
    preparedStatement.setLong(1, partitionDependency.getPartitionId());
    preparedStatement.setLong(2, partitionDependency.getDependencyPartitionId());
    preparedStatement.executeUpdate();
    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  private static String INSERT_DT_SOURCE =
      "INSERT INTO UHRWERK_METASTORE.DT_SOURCE(cf_table_id, connection_id, sql_select_query, sql_partition_query, partition_column, query_column, version, description) VALUES (?,?,?,?,?,?,?,?,?)";

  public static Long save(Connection db, DataSource source) throws SQLException {
    PreparedStatement preparedStatement =
        db.prepareStatement(INSERT_DT_SOURCE, Statement.RETURN_GENERATED_KEYS);
    preparedStatement.setLong(1, source.getCfTableId());
    preparedStatement.setLong(2, source.getConnectionId());
    preparedStatement.setString(3, source.getSqlSelectQuery());
    preparedStatement.setString(4, source.getSqlPartitionQuery());
    preparedStatement.setString(5, source.getPartitionColumn());
    preparedStatement.setString(6, source.getQueryColumn());
    preparedStatement.setString(7, source.getVersion());
    preparedStatement.setString(8, source.getDescription());
    preparedStatement.executeUpdate();
    ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }
}
