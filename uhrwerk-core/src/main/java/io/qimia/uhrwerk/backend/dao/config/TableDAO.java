package io.qimia.uhrwerk.backend.dao.config;

import io.qimia.uhrwerk.backend.dao.data.DependencyDAO;
import io.qimia.uhrwerk.backend.dao.data.SourceDAO;
import io.qimia.uhrwerk.backend.dao.data.TargetDAO;
import io.qimia.uhrwerk.config.PartitionUnit;
import io.qimia.uhrwerk.config.model.Source;
import io.qimia.uhrwerk.config.model.Table;
import io.qimia.uhrwerk.config.model.Target;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TableDAO {
  private static String INSERT =
      "INSERT INTO TABLE_(area, vertical, table_name, batch_temporal_unit, batch_size, parallelism, max_partitions, version) VALUES(?,?,?,?,?,?,?,?)";

  private static String SELECT_BY_ID =
      "SELECT id, area, vertical, table_name, batch_temporal_unit, batch_size, parallelism, max_partitions, version, created_ts, updated_ts FROM TABLE_ WHERE id = ?";

  public static Long save(java.sql.Connection db, Table table) throws SQLException {
    Long tableId = null;
    db.setAutoCommit(false);
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    insert.setString(1, table.getArea());
    insert.setString(2, table.getVertical());
    insert.setString(3, table.getName());
    insert.setString(4, table.getPartitionSizeType().name());
    insert.setInt(5, table.getPartitionSizeInt());
    insert.setInt(6, table.getParallelism());
    insert.setInt(7, table.getMaxBatches());
    insert.setString(8, table.getVersion());
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) tableId = generatedKeys.getLong(1);

    if (table.getTargets() != null && table.getTargets().length > 0) {
      for (Target target : table.getTargets()) {
        target.setTableId(tableId);
      }
      TargetDAO.save(db, table.getTargets());
    }
    if (table.getDependencies() != null && table.getDependencies().length > 0) {
      Dependency[] dependencies = table.getDependencies();
      for (Dependency dependency : table.getDependencies()) {
        dependency.setTableId(tableId);
      }
      DependencyDAO.save(db, dependencies);
    }
    if (table.getSources() != null && table.getSources().length > 0) {
      Source[] sources = table.getSources();
      for (Source source : table.getSources()) {
        source.setCfTableId(tableId);
      }
      SourceDAO.save(db, table.getSources());
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
      res.setName(record.getString(4));
      res.setPartitionSizeType(PartitionUnit.valueOf(record.getString(5)));
      res.setPartitionSizeInt(record.getInt(6));
      res.setParallelism(record.getInt(7));
      res.setMaxBatches(record.getInt(8));
      res.setVersion(record.getString(9));
      res.setCreatedTs(record.getTimestamp(10).toLocalDateTime());
      res.setUpdatedTs(record.getTimestamp(11).toLocalDateTime());
      return res;
    }

    return null;
  }
}
