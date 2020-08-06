package io.qimia.uhrwerk.models.dao;

import io.qimia.uhrwerk.models.db.config.ConfigConnection;
import io.qimia.uhrwerk.models.db.config.ConfigTable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ConfigDAO {

  private static String INSERT_CF_CONNECTION =
      "INSERT INTO "
          + "UHRWERK_METASTORE.CF_CONNECTION(connection_name, connection_type, connection_url, version, description) VALUES(?,?,?,?,?)";

  public static Long saveConnection(java.sql.Connection db, ConfigConnection configConnection)
      throws SQLException {
    PreparedStatement insert =
        db.prepareStatement(INSERT_CF_CONNECTION, Statement.RETURN_GENERATED_KEYS);
    insert.setString(1, configConnection.getConnectionName());
    insert.setString(2, configConnection.getConnectionType());
    insert.setString(3, configConnection.getConnectionUrl());
    insert.setString(4, configConnection.getVersion());
    insert.setString(5, configConnection.getDescription());
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  private static String INSERT_CF_TABLE =
      "INSERT INTO UHRWERK_METASTORE.CF_TABLE(area, vertical, table_name, batch_temporal_unit, batch_size, parallelism, max_partitions, version) VALUES(?,?,?,?,?,?,?,?)";

  public Long saveTable(java.sql.Connection db, ConfigTable configTable) throws SQLException {
    PreparedStatement insert =
        db.prepareStatement(INSERT_CF_TABLE, Statement.RETURN_GENERATED_KEYS);
    insert.setString(1, configTable.getArea());
    insert.setString(2, configTable.getVertical());
    insert.setString(3, configTable.getTableName());
    insert.setString(4, configTable.getBatchTemporalUnit().name());
    insert.setInt(5, configTable.getBatchSize());
    insert.setInt(6, configTable.getParallelism());
    insert.setInt(7, configTable.getMaxPartitions());
    insert.setString(7, configTable.getVersion());
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }
}
