package io.qimia.uhrwerk.backend.dao.data;

import io.qimia.uhrwerk.config.model.Dependency;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DependencyDAO {
  private static final String SELECT_BY_ID = "";
  private static String INSERT =
      "INSERT INTO "
          + "DEPENDENCY(table_id, target_id, transform_type, transform_partition_unit, transform_partition_size)"
          + " VALUES (?,?,?,?,?)";

  public static Long save(
      java.sql.Connection db, Dependency dependency, Long tableId, Long targetId)
      throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    setInsertParams(dependency, tableId, targetId, insert);
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  public static List<Long> save(
      java.sql.Connection db, Dependency[] dependencies, Long tableId, Long targetId)
      throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    for (int i = 0; i < dependencies.length; i++) {
      setInsertParams(dependencies[i], tableId, targetId, insert);
      insert.addBatch();
    }
    insert.executeBatch();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    List<Long> ids = new ArrayList<>(dependencies.length);
    while (generatedKeys.next()) ids.add(generatedKeys.getLong(1));
    return ids;
  }

  private static void setInsertParams(
      Dependency dependency, Long tableId, Long targetId, PreparedStatement insert)
      throws SQLException {
    insert.setLong(1, tableId);
    insert.setLong(2, targetId);
    insert.setString(3, dependency.getTransformType().name());
    insert.setString(4, dependency.getTransformPartitionUnit().name());
    insert.setInt(5, dependency.getTransformPartitionSize());
  }
}
