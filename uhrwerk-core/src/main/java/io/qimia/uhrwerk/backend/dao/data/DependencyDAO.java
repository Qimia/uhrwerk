package io.qimia.uhrwerk.backend.dao.data;


import io.qimia.uhrwerk.config.model.Dependency;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DependencyDAO {
  private static String INSERT =
      "INSERT INTO DEPENDENCY(table_id, target_id, partition_transform, batch_temporal_unit, batch_size)  VALUES (?,?,?,?,?)";

  public static Long save(java.sql.Connection db, Dependency dependency) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    setInsertParams(dependency, insert);
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  private static void setInsertParams(Dependency dependency, PreparedStatement insert)
      throws SQLException {
    insert.setLong(1, dependency.getTableId());
    insert.setLong(2, dependency.getTargetId());
    insert.setString(3, dependency.getPartitionTransform().name());
    insert.setString(4, dependency.getBatchTemporalUnit().name());
    insert.setInt(5, dependency.getBatchSize());
  }

  public static List<Long> save(java.sql.Connection db, List<Dependency> dependencies)
      throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    for (Iterator<Dependency> iterator = dependencies.iterator(); iterator.hasNext(); ) {
      Dependency dependency = iterator.next();
      setInsertParams(dependency, insert);
      insert.addBatch();
    }
    insert.executeBatch();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    List<Long> ids = new ArrayList<>(dependencies.size());
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
