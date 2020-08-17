package io.qimia.uhrwerk.dao;


import io.qimia.uhrwerk.common.model.Source;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SourceDAO {
  private static String INSERT_STR =
      "INSERT INTO SOURCE(table_id, connection_id, path, sql_select_query, sql_partition_query, partition_column, query_column, partition_num,partition_unit,partition_size )\n"
          + "SELECT %d,cn.id,%s,%s,%s,%s,%s,%d,%s,%d FROM CONNECTION cn\n"
          + "WHERE cn.name = '%s'";

  public static Long save(java.sql.Connection db, Source source, Long tableId) throws SQLException {
    String insertStr = setInsertParams(source, tableId);
    PreparedStatement insert = db.prepareStatement(insertStr, Statement.RETURN_GENERATED_KEYS);
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  private static String setInsertParams(Source source, Long tableId) throws SQLException {
    return String.format(
        INSERT_STR,
        tableId,
        source.getPath(),
        source.getSelectQuery(),
        source.getParallelLoadQuery(),
        source.getParallelLoadColumn(),
        source.getSelectColumn(),
        source.getParallelLoadNum(),
        source.getPartitionUnit().name(),
        source.getPartitionSize());
  }

  public static List<Long> save(java.sql.Connection db, Source[] sources, Long tableId)
      throws SQLException {
    Statement statement = db.createStatement();
    for (Source source : sources) {
      String insertStr = setInsertParams(source, tableId);
      statement.addBatch(insertStr);
    }
    statement.executeBatch();
    ResultSet generatedKeys = statement.getGeneratedKeys();
    List<Long> ids = new ArrayList<>(sources.length);
    while (generatedKeys.next()) ids.add(generatedKeys.getLong(1));
    return ids;
  }
}
