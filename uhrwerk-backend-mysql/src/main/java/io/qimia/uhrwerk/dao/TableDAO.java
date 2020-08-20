package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.dependency.TableDependencyService;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResultSet;
import io.qimia.uhrwerk.common.model.*;

import java.sql.*;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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
      new DependencyDAO(db)
          .save(
              table.getDependencies(),
              tableId,
              table.getPartitionUnit(),
              table.getPartitionSize(),
              true);
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

  public static final String SELECT_TABLE_PARTITION_SPEC =
      "SELECT D.id,\n"
          + "       T.id,\n"
          + "       T.partition_unit,\n"
          + "       T.partition_size,\n"
          + "       d.transform_type,\n"
          + "       d.transform_partition_unit,\n"
          + "       d.transform_partition_size\n"
          + "FROM TABLE_ T\n"
          + "         JOIN (SELECT d.id, d.dependency_table_id, transform_type, transform_partition_unit, transform_partition_size\n"
          + "               FROM TABLE_ t\n"
          + "                        JOIN DEPENDENCY d on t.id = d.table_id\n"
          + "               WHERE t.id = ?) AS D ON D.dependency_table_id = T.id\n"
          + "ORDER BY D.id";

  @Override
  public TablePartitionResultSet processingPartitions(Table table, LocalDateTime[] partitionTs)
      throws SQLException {
    PreparedStatement partitionSpecs =
        getDb().prepareStatement(SELECT_TABLE_PARTITION_SPEC, Statement.RETURN_GENERATED_KEYS);
    partitionSpecs.setLong(1, table.getId());
    List<TablePartitionSpec> tablePartitionSpecs = new ArrayList<>();
    ResultSet resultSet = partitionSpecs.executeQuery();
    while (resultSet.next()) {
      long dependencyId = resultSet.getLong(1);
      long tableId = resultSet.getLong(2);
      PartitionUnit partitionUnit = PartitionUnit.valueOf(resultSet.getString(3));
      int partitionSize = resultSet.getInt(4);
      PartitionTransformType transformType = PartitionTransformType.valueOf(resultSet.getString(5));
      PartitionUnit transformUnit = PartitionUnit.valueOf(resultSet.getString(6));
      int transformSize = resultSet.getInt(7);
      tablePartitionSpecs.add(
          new TablePartitionSpec(
              dependencyId,
              tableId,
              partitionUnit,
              partitionSize,
              transformType,
              transformUnit,
              transformSize));
    }

    for (TablePartitionSpec spec : tablePartitionSpecs) {
      LocalDateTime[][] localDateTimes =
          JdbcBackendUtils.dependencyPartitions(
              partitionTs,
              table.getPartitionUnit(),
              table.getPartitionSize(),
              spec.partitionUnit,
              spec.partitionSize,
              spec.transformType,
              spec.transformSize,
              spec.transformUnit);
    }
    return null;
  }

  private static class TablePartitionSpec {
    private Long dependencyId;
    private Long tableId;
    private PartitionUnit partitionUnit = null;
    private Integer partitionSize = null;
    private PartitionTransformType transformType = null;
    private PartitionUnit transformUnit = null;
    private Integer transformSize = null;

    public TablePartitionSpec(
        Long dependencyId,
        Long tableId,
        PartitionUnit partitionUnit,
        Integer partitionSize,
        PartitionTransformType transformType,
        PartitionUnit transformUnit,
        Integer transformSize) {
      this.dependencyId = dependencyId;
      this.tableId = tableId;
      this.partitionUnit = partitionUnit;
      this.partitionSize = partitionSize;
      this.transformType = transformType;
      this.transformUnit = transformUnit;
      this.transformSize = transformSize;
    }

    public Long getDependencyId() {
      return dependencyId;
    }

    public Long getTableId() {
      return tableId;
    }

    public PartitionUnit getPartitionUnit() {
      return partitionUnit;
    }

    public Integer getPartitionSize() {
      return partitionSize;
    }

    public PartitionTransformType getTransformType() {
      return transformType;
    }

    public PartitionUnit getTransformUnit() {
      return transformUnit;
    }

    public Integer getTransformSize() {
      return transformSize;
    }

    @Override
    public String toString() {
      return "TablePartitionSpec{"
          + "dependencyId="
          + dependencyId
          + ", tableId="
          + tableId
          + ", partitionUnit="
          + partitionUnit
          + ", partitionSize="
          + partitionSize
          + ", transformType="
          + transformType
          + ", transformUnit="
          + transformUnit
          + ", transformSize="
          + transformSize
          + '}';
    }
  }
}
