package io.qimia.uhrwerk.backend.dao.data;

import io.qimia.uhrwerk.backend.model.data.PartitionDependency;
import io.qimia.uhrwerk.backend.model.data.Source;
import io.qimia.uhrwerk.backend.model.data.Target;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PartitionDependencyDAO {
  private static String INSERT =
      "INSERT INTO DT_PARTITION_DEPENDENCY(partition_id, dependency_partition_id) VALUES (?,?)";

  private static String SELECT_BY_ID =
      "SELECT id, cf_table_id, cf_connection_id, path, created_ts, updated_ts FROM DT_TARGET WHERE id = ?";

  public static Long save(java.sql.Connection db, PartitionDependency partitionDependency)
      throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    setInsertParams(partitionDependency, insert);
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  private static void setInsertParams(
      PartitionDependency partitionDependency, PreparedStatement insert) throws SQLException {
    insert.setLong(1, partitionDependency.getPartitionId());
    insert.setLong(2, partitionDependency.getDependencyPartitionId());
  }

  public static List<Long> save(
      java.sql.Connection db, List<PartitionDependency> partitionDependencies) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    for (Iterator<PartitionDependency> iterator = partitionDependencies.iterator();
        iterator.hasNext(); ) {
      PartitionDependency partitionDependency = iterator.next();
      setInsertParams(partitionDependency, insert);
      insert.addBatch();
    }
    insert.executeBatch();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    List<Long> ids = new ArrayList<>(partitionDependencies.size());
    while (generatedKeys.next()) ids.add(generatedKeys.getLong(1));
    return ids;
  }

  public static Target get(java.sql.Connection db, Long id) throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_BY_ID);
    select.setLong(1, id);

    ResultSet record = select.executeQuery();

    if (record.next()) {
      return null;
    }

    return null;
  }
}
