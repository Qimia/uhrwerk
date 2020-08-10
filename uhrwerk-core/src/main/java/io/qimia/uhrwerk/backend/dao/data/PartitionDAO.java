package io.qimia.uhrwerk.backend.dao.data;

import io.qimia.uhrwerk.backend.model.data.Partition;
import io.qimia.uhrwerk.backend.model.data.Target;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PartitionDAO {
  private static String INSERT =
      "INSERT INTO DT_PARTITION(dt_target_id, path, year, month, day, hour, minute, partition_hash)  VALUES (?,?,?,?,?,?,?,?)";

  private static String SELECT_BY_ID =
      "SELECT id, dt_target_id, path, year, month, day, hour, minute, partition_hash, created_ts, updated_ts FROM DT_PARTITION WHERE id=?";

  public static Long save(java.sql.Connection db, Partition partition) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    setInsertParams(partition, insert);
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  private static void setInsertParams(Partition dependency, PreparedStatement insert)
      throws SQLException {
    insert.setLong(1, dependency.getDtTargetId());
    insert.setString(2, dependency.getPath());
    insert.setString(3, dependency.getYear());
    insert.setString(4, dependency.getMonth());
    insert.setString(5, dependency.getDay());
    insert.setString(6, dependency.getHour());
    insert.setString(7, dependency.getMinute());
    insert.setString(7, dependency.getPartitionHash());
  }

  public static List<Long> save(java.sql.Connection db, List<Partition> partitions)
      throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    for (Iterator<Partition> iterator = partitions.iterator(); iterator.hasNext(); ) {
      Partition partition = iterator.next();
      setInsertParams(partition, insert);
      insert.addBatch();
    }
    insert.executeBatch();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    List<Long> ids = new ArrayList<>(partitions.size());
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
