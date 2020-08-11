package io.qimia.uhrwerk.backend.dao.data;

import io.qimia.uhrwerk.config.model.Target;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TargetDAO {
  private static String INSERT =
      "INSERT INTO TARGET(table_id, connection_id, format) VALUES (?,?,?)";

  private static String SELECT_BY_ID =
      "SELECT id, table_id, connection_id, format, created_ts, updated_ts FROM TARGET WHERE id = ?";

  public static Long save(java.sql.Connection db, Target target) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    insert.setLong(1, target.getTableId());
    insert.setLong(2, target.getConnectionId());
    insert.setString(3, target.getFormat());
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  public static List<Long> save(java.sql.Connection db, Target[] targets) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    for (int i = 0; i < targets.length; i++) {
      insert.setLong(1, targets[i].getTableId());
      insert.setLong(2, targets[i].getConnectionId());
      insert.setString(3, targets[i].getFormat());
      insert.addBatch();
    }
    insert.executeBatch();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    List<Long> ids = new ArrayList<>(targets.length);
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
