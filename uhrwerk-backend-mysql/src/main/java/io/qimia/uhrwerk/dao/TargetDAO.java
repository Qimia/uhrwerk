package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.Target;

import java.sql.*;

public class TargetDAO {

  java.sql.Connection db;

  public TargetDAO() {}

  public TargetDAO(Connection db) {
    this.db = db;
  }

  public Connection getDb() {
    return db;
  }

  public void setDb(Connection db) {
    this.db = db;
  }

  private static String INSERT_TMP =
      "INSERT INTO TARGET (table_id, connection_id, format)\n"
          + "SELECT %d, cn.id,'%s'\n"
          + " FROM CONNECTION cn\n"
          + " WHERE cn.name = '%s'";

  private static String SELECT_BY_ID =
      "SELECT id, table_id, connection_id, format, created_ts, updated_ts FROM TARGET WHERE id = ?";

  public Long save(Target target, Long tableId) throws SQLException {
    String insertStr =
        String.format(INSERT_TMP, tableId, target.getFormat(), target.getConnection().getName());
    PreparedStatement insert = getDb().prepareStatement(insertStr, Statement.RETURN_GENERATED_KEYS);
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  public Long save(java.sql.Connection db, Target[] targets, Long tableId) throws SQLException {
    Statement statement = db.createStatement();
    for (Target target : targets) {
      String insert =
          String.format(INSERT_TMP, tableId, target.getFormat(), target.getConnection().getName());
      statement.addBatch(insert);
    }
    statement.executeBatch();
    ResultSet generatedKeys = statement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  public Target get(Long id) throws SQLException {
    PreparedStatement select = getDb().prepareStatement(SELECT_BY_ID);
    select.setLong(1, id);

    ResultSet record = select.executeQuery();

    if (record.next()) {
      return null;
    }

    return null;
  }
}
