package io.qimia.uhrwerk.backend.dao.config;

import io.qimia.uhrwerk.backend.dao.data.TargetDAO;
import io.qimia.uhrwerk.backend.model.BatchTemporalUnit;
import io.qimia.uhrwerk.backend.model.config.Table;
import io.qimia.uhrwerk.backend.model.data.Target;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TableDAO {
  private static String INSERT =
      "INSERT INTO CF_TABLE(area, vertical, table_name, batch_temporal_unit, batch_size, parallelism, max_partitions, version) VALUES(?,?,?,?,?,?,?,?)";

  private static String SELECT_BY_ID =
      "SELECT id, area, vertical, table_name, batch_temporal_unit, batch_size, parallelism, max_partitions, version, created_ts, updated_ts FROM CF_TABLE WHERE id = ?";

  public static Long save(java.sql.Connection db, Table table) throws SQLException {
    Long tableId = null;
    db.setAutoCommit(false);
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    insert.setString(1, table.getArea());
    insert.setString(2, table.getVertical());
    insert.setString(3, table.getTableName());
    insert.setString(4, table.getBatchTemporalUnit().name());
    insert.setInt(5, table.getBatchSize());
    insert.setInt(6, table.getParallelism());
    insert.setInt(7, table.getMaxPartitions());
    insert.setString(8, table.getVersion());
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) tableId = generatedKeys.getLong(1);

    if (table.getTargets() != null && !table.getTargets().isEmpty()) {
      for (Target target : table.getTargets()) {
        target.setCfTableId(tableId);
      }
      TargetDAO.save(db, table.getTargets());
    }
    db.commit();
    return tableId;
  }

  public static Table get(java.sql.Connection db, Long id) throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_BY_ID);
    select.setLong(1, id);

    ResultSet record = select.executeQuery();

    if (record.next()) {
      Table res = new Table();
      res.setId(record.getLong(1));
      res.setArea(record.getString(2));
      res.setVertical(record.getString(3));
      res.setTableName(record.getString(4));
      res.setBatchTemporalUnit(BatchTemporalUnit.valueOf(record.getString(5)));
      res.setBatchSize(record.getInt(6));
      res.setParallelism(record.getInt(7));
      res.setMaxPartitions(record.getInt(8));
      res.setVersion(record.getString(9));
      res.setCreatedTs(record.getTimestamp(10));
      res.setUpdatedTs(record.getTimestamp(11));
      return res;
    }

    return null;
  }
}
