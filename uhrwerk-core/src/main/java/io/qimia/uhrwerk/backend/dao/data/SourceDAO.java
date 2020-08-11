package io.qimia.uhrwerk.backend.dao.data;

import io.qimia.uhrwerk.config.model.Source;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SourceDAO {
  private static String INSERT =
      "INSERT INTO SOURCE(table_id, connection_id, sql_select_query, sql_partition_query, partition_column, query_column)  VALUES (?,?,?,?,?,?,?,?)";

  private static String SELECT_BY_ID =
      "SELECT id, table_id, connection_id, format, created_ts, updated_ts FROM TARGET WHERE id = ?";

  public static Long save(java.sql.Connection db, Source source) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    setInsertParams(source, insert);
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  private static void setInsertParams(Source source, PreparedStatement insert) throws SQLException {
    insert.setLong(1, source.getCfTableId());
    insert.setLong(2, source.getConnectionId());
    insert.setString(3, source.getSelectQuery());
    insert.setString(4, source.getPartitionQuery());
    insert.setString(5, source.getPartitionColumn());
    insert.setString(6, source.getQueryColumn());
  }

  public static List<Long> save(java.sql.Connection db, Source[] sources) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    for (Source source : sources) {
      setInsertParams(source, insert);
      insert.addBatch();
    }
    insert.executeBatch();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    List<Long> ids = new ArrayList<>(sources.length);
    while (generatedKeys.next()) ids.add(generatedKeys.getLong(1));
    return ids;
  }

  public static Source get(java.sql.Connection db, Long id) throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_BY_ID);
    select.setLong(1, id);

    ResultSet record = select.executeQuery();

    if (record.next()) {
      return null;
    }

    return null;
  }
}
