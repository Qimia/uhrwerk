package io.qimia.uhrwerk.backend.dao.data;

import io.qimia.uhrwerk.backend.model.data.Dependency;
import io.qimia.uhrwerk.backend.model.data.Target;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DependencyDAO {
  private static String INSERT =
      "INSERT INTO DT_DEPENDENCY(cf_table_id, dt_target_id, partition_transform, batch_temporal_unit, batch_size)  VALUES (?,?,?,?,?)";

  private static String SELECT_BY_ID =
      "SELECT id, cf_table_id, cf_connection_id, path, created_ts, updated_ts FROM DT_TARGET WHERE id = ?";

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
    insert.setLong(1, dependency.getCfTableId());
    insert.setLong(2, dependency.getDtTargetId());
    insert.setString(3, dependency.getPartitionTransform().name());
    insert.setString(4, dependency.getBatchTemporalUnit().name());
    insert.setInt(5, dependency.getBatchSize());
  }

  public static List<Long> save(java.sql.Connection db, List<Dependency> dependencies)
      throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    for (Iterator<Dependency> iterator = dependencies.iterator(); iterator.hasNext(); ) {
      Dependency target = iterator.next();
      setInsertParams(target, insert);
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
