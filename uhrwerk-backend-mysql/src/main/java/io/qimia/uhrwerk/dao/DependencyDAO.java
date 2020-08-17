package io.qimia.uhrwerk.dao;


import io.qimia.uhrwerk.common.model.Dependency;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DependencyDAO {
  private static final String SELECT_BY_ID = "";
  private static String INSERT_STR =
      "INSERT INTO DEPENDENCY(table_id, target_id, transform_type, transform_partition_unit, transform_partition_size)\n"
          + "SELECT %d,tr.id, '%s', '%s', %d FROM TABLE_ tl\n"
          + "JOIN TARGET tr on tl.id = tr.table_id\n"
          + "WHERE tl.area = '%s'\n"
          + "AND tl.vertical = '%s'\n"
          + "AND  tl.name = '%s'\n"
          + "AND tl.version = '%s'\n"
          + "AND tr.format = '%s'";

  public static Long save(java.sql.Connection db, Dependency dependency, Long tableId)
      throws SQLException {
    String insertStr = setInsertParams(dependency, tableId);
    PreparedStatement insert = db.prepareStatement(insertStr);
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  public static List<Long> save(java.sql.Connection db, Dependency[] dependencies, Long tableId)
      throws SQLException {
    Statement statement = db.createStatement();
    for (int i = 0; i < dependencies.length; i++) {
      String insertStr = setInsertParams(dependencies[i], tableId);
      statement.addBatch(insertStr);
    }
    statement.executeBatch();
    ResultSet generatedKeys = statement.getGeneratedKeys();
    List<Long> ids = new ArrayList<>(dependencies.length);
    while (generatedKeys.next()) ids.add(generatedKeys.getLong(1));
    return ids;
  }

  private static String setInsertParams(Dependency dependency, Long tableId) {
    return String.format(
        INSERT_STR,
        tableId,
        dependency.getTransformType().name(),
        dependency.getTransformPartitionUnit().name(),
        dependency.getTransformPartitionSize(),
        dependency.getArea(),
        dependency.getVertical(),
        dependency.getTableName(),
        dependency.getVersion(),
        dependency.getFormat());
  }
}
