package io.qimia.uhrwerk.backend.dao.data;

import io.qimia.uhrwerk.backend.model.data.Partition;
import io.qimia.uhrwerk.backend.model.data.Source;
import io.qimia.uhrwerk.backend.model.data.Target;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SourceDAO {
  private static String INSERT =
      "INSERT INTO DT_SOURCE(cf_table_id, connection_id, sql_select_query, sql_partition_query, partition_column, query_column)  VALUES (?,?,?,?,?,?,?,?)";

  private static String SELECT_BY_ID =
      "SELECT id, cf_table_id, cf_connection_id, path, created_ts, updated_ts FROM DT_TARGET WHERE id = ?";

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
    insert.setString(3, source.getSqlSelectQuery());
    insert.setString(4, source.getSqlPartitionQuery());
    insert.setString(5, source.getPartitionColumn());
    insert.setString(6, source.getQueryColumn());
  }

  public static List<Long> save(java.sql.Connection db, List<Source> sources) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    for (Iterator<Source> iterator = sources.iterator(); iterator.hasNext(); ) {
      Source source = iterator.next();
      setInsertParams(source, insert);
      insert.addBatch();
    }
    insert.executeBatch();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    List<Long> ids = new ArrayList<>(sources.size());
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
