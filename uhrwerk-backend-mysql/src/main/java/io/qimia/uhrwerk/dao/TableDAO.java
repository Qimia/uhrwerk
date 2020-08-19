package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.dependency.TableDependencyService;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResultSet;
import io.qimia.uhrwerk.common.model.Table;
import io.qimia.uhrwerk.common.model.Target;

import java.sql.*;
import java.time.LocalDateTime;

public class TableDAO implements TableDependencyService {

  java.sql.Connection db;

  public TableDAO() {}

  public TableDAO(Connection db) {
    this.db = db;
  }

  public Connection getDb() {
    return db;
  }

  public void setDb(Connection db) {
    this.db = db;
  }

  private static String UPSERT =
          "INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size) "
                  + "VALUES(?,?,?,?,?,?,?,?,?) "
                  + "ON DUPLICATE KEY UPDATE "
                  + "parallelism=?, "
                  + "max_bulk_size=?";

  public Long save(Table table) throws SQLException {
    db.setAutoCommit(false);
    Long tableId = saveTable(table);

    if (table.getTargets() != null && table.getTargets().length > 0) {
      for (Target target : table.getTargets()) {
        new TargetDAO(db).save(target, tableId);
      }
    }
    if (table.getDependencies() != null && table.getDependencies().length > 0) {
      new DependencyDAO(db).save(
              table.getDependencies(),
              tableId,
              table.getPartitionUnit(),
              table.getPartitionSize(),
              true
      );
    }
    if (table.getSources() != null && table.getSources().length > 0) {
      new SourceDAO(db).save(table.getSources(), true); // todo pass overwrite
    }
    db.commit();
    return tableId;
  }

  public Long saveTable(Table table) throws SQLException {
    PreparedStatement insert = getDb().prepareStatement(UPSERT, Statement.RETURN_GENERATED_KEYS);
    insert.setLong(1, table.getId());
    insert.setString(2, table.getArea());
    insert.setString(3, table.getVertical());
    insert.setString(4, table.getName());
    insert.setString(5, table.getVersion());
    insert.setString(6, table.getPartitionUnit().name());
    insert.setInt(7, table.getPartitionSize());
    insert.setInt(8, table.getParallelism());
    insert.setInt(9, table.getMaxBulkSize());
    // assignment_list
    insert.setInt(10, table.getParallelism());
    insert.setInt(11, table.getMaxBulkSize());
    insert.executeUpdate();
    ResultSet generatedKeys = insert.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }

  public Table get(Long id) throws SQLException {
    PreparedStatement select = getDb().prepareStatement("SELECT * FROM TABLE_ WHERE id=1");
    return null;
  }

  @Override
  public TablePartitionResultSet processingPartitions(Table table, LocalDateTime[] partitionTs) {
    return null;
  }
}
