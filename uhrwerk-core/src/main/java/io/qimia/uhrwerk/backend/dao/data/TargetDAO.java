package io.qimia.uhrwerk.backend.dao.data;

import io.qimia.uhrwerk.backend.model.data.Target;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TargetDAO {
  private static String INSERT =
      "INSERT INTO DT_TARGET(cf_table_id, cf_connection_id, path) VALUES (?,?,?)";

  private static String SELECT_BY_ID =
      "SELECT id, cf_table_id, cf_connection_id, path, created_ts, updated_ts FROM DT_TARGET WHERE id = ?";

  public static Long save(java.sql.Connection db, Target target) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    insert.setLong(1, target.getCfTableId());
    insert.setLong(2, target.getCfConnectionId());
    insert.setString(3, target.getPath());
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  public static List<Long> save(java.sql.Connection db, List<Target> targets) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    for (Iterator<Target> iterator = targets.iterator(); iterator.hasNext(); ) {
      Target target = iterator.next();
      insert.setLong(1, target.getCfTableId());
      insert.setLong(2, target.getCfConnectionId());
      insert.setString(3, target.getPath());
      insert.addBatch();
    }
    insert.executeBatch();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    List<Long> ids = new ArrayList<>(targets.size());
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
