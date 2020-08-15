package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.dependency.TableDependencyService;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResultSet;
import io.qimia.uhrwerk.common.model.Table;
import io.qimia.uhrwerk.common.model.Target;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;

public class TableDAO implements TableDependencyService {
  private static String INSERT =
      "INSERT INTO TABLE_(area, vertical, name, partition_unit, partition_size, parallelism, max_bulk_size, version) VALUES(?,?,?,?,?,?,?,?)";

  public static Long save(java.sql.Connection db, Table table) throws SQLException {
    Long tableId = null;
    db.setAutoCommit(false);
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    insert.setString(1, table.getArea());
    insert.setString(2, table.getVertical());
    insert.setString(3, table.getName());
    insert.setString(4, table.getPartitionUnit().name());
    insert.setInt(5, table.getPartitionSize());
    insert.setInt(6, table.getParallelism());
    insert.setInt(7, table.getMaxBulkSize());
    insert.setString(8, table.getVersion());
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) tableId = generatedKeys.getLong(1);

    if (table.getTargets() != null && table.getTargets().length > 0) {
      for (Target target : table.getTargets()) {
        TargetDAO.save(db, target, tableId);
      }
    }
    if (table.getDependencies() != null && table.getDependencies().length > 0) {
      DependencyDAO.save(db, table.getDependencies(), tableId);
    }
    if (table.getSources() != null && table.getSources().length > 0) {
      SourceDAO.save(db, table.getSources(), tableId);
    }
    db.commit();
    return tableId;
  }

  public static Table get(java.sql.Connection db, Long id) throws SQLException {
    PreparedStatement select = db.prepareStatement("SELECT * FROM TABLE_ WHERE id=1");
    return null;
  }

  @Override
  public TablePartitionResultSet processingPartitions(Table table, LocalDateTime[] partitionTs) {
    return null;
  }
}
