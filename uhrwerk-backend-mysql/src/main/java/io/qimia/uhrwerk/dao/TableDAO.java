package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult;
import io.qimia.uhrwerk.common.metastore.dependency.TableDependencyService;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResultSet;
import io.qimia.uhrwerk.common.model.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;

public class TableDAO implements TableDependencyService {

  java.sql.Connection db;
  PartitionDAO partitionDAO;
  ConnectionDAO connectionDAO;

  public TableDAO() {}

  public TableDAO(java.sql.Connection db) {
    this.db = db;
    this.partitionDAO = new PartitionDAO(db);
    this.connectionDAO = new ConnectionDAO(db);
  }

  public java.sql.Connection getDb() {
    return db;
  }

  public void setDb(java.sql.Connection db) {
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
          + "       D.dependency_target_id,\n"
          + "       T.partition_unit,\n"
          + "       T.partition_size,\n"
          + "       d.transform_type,\n"
          + "       d.transform_partition_unit,\n"
          + "       d.transform_partition_size,\n"
          + "       TR.connection_id\n"
          + "FROM TABLE_ T\n"
          + "         JOIN (SELECT d.id,\n"
          + "                      d.dependency_table_id,\n"
          + "                      d.dependency_target_id,\n"
          + "                      transform_type,\n"
          + "                      transform_partition_unit,\n"
          + "                      transform_partition_size\n"
          + "               FROM TABLE_ t\n"
          + "                        JOIN DEPENDENCY d on t.id = d.table_id\n"
          + "               WHERE t.id = ?) AS D ON D.dependency_table_id = T.id\n"
          + "         JOIN TARGET TR on TR.id = D.dependency_target_id\n"
          + "ORDER BY D.id";

  @Override
  public TablePartitionResultSet processingPartitions(
      Table table, LocalDateTime[] requestedPartitionTs) throws SQLException {
    List<TablePartitionSpec> tablePartitionSpecs = getTablePartitionSpecs(table);
    Connection[] connections = this.connectionDAO.getTableDependenciesConnections(table.getId());
    Map<Long, Connection> connectionsMap = new HashMap<>();
    for (int i = 0; i < connections.length; i++) {
      connectionsMap.put(connections[i].getId(), connections[i]);
    }
    Map<Long, Dependency> dependenciesMap = new HashMap<>();
    for (int i = 0; i < table.getDependencies().length; i++) {
      dependenciesMap.put(table.getDependencies()[i].getId(), table.getDependencies()[i]);
    }

    for (TablePartitionSpec spec : tablePartitionSpecs) {
      LocalDateTime[][] partitionTs =
          JdbcBackendUtils.dependencyPartitions(
              requestedPartitionTs,
              table.getPartitionUnit(),
              table.getPartitionSize(),
              spec.partitionUnit,
              spec.partitionSize,
              spec.transformType,
              spec.transformSize,
              spec.transformUnit);
      for (int i = 0; i < partitionTs.length; i++) {
        TablePartitionSpec tablePartitionSpec = tablePartitionSpecs.get(i);
        Partition[] depPartitions =
            partitionDAO.getPartitions(tablePartitionSpec.getTargetId(), partitionTs[i]);
        DependencyResult dependencyResult = new DependencyResult();
        dependencyResult.setConnection(connectionsMap.get(tablePartitionSpec.connectionId));
        dependencyResult.setDependency(dependenciesMap.get(tablePartitionSpec.dependencyId));
        if (depPartitions != null
            && depPartitions.length == tablePartitionSpec.getTransformSize()) {
          dependencyResult.setSuccess(true);
          dependencyResult.setSucceeded(partitionTs[i]);
          dependencyResult.setPartitions(depPartitions);
        } else {
          List<LocalDateTime> succeeded = new ArrayList<>();
          if (depPartitions != null)
            for (Partition partition : depPartitions) {
              LocalDateTime ts = partition.getPartitionTs();
              succeeded.add(ts);
            }
          TreeSet<LocalDateTime> failed = new TreeSet<>(Arrays.asList(partitionTs[i]));
          failed.removeAll(succeeded);
          dependencyResult.setSuccess(false);
          dependencyResult.setFailed(failed.toArray(new LocalDateTime[failed.size()]));
          dependencyResult.setSucceeded(succeeded.toArray(new LocalDateTime[succeeded.size()]));
        }
        // TablePartitionResult tablePartitionResult = new TablePartitionResult();

      }
    }
    return null;
  }

  private List<TablePartitionSpec> getTablePartitionSpecs(Table table) throws SQLException {
    PreparedStatement partitionSpecs =
        getDb().prepareStatement(SELECT_TABLE_PARTITION_SPEC, Statement.RETURN_GENERATED_KEYS);
    partitionSpecs.setLong(1, table.getId());
    List<TablePartitionSpec> tablePartitionSpecs = new ArrayList<>();
    ResultSet resultSet = partitionSpecs.executeQuery();
    while (resultSet.next()) {
      long dependencyId = resultSet.getLong(1);
      long targetId = resultSet.getLong(2);
      long tableId = resultSet.getLong(3);
      PartitionUnit partitionUnit = PartitionUnit.valueOf(resultSet.getString(4));
      int partitionSize = resultSet.getInt(5);
      PartitionTransformType transformType = PartitionTransformType.valueOf(resultSet.getString(6));
      PartitionUnit transformUnit = PartitionUnit.valueOf(resultSet.getString(7));
      int transformSize = resultSet.getInt(8);
      long connectionId = resultSet.getLong(9);
      tablePartitionSpecs.add(
          new TablePartitionSpec(
              dependencyId,
              targetId,
              tableId,
              partitionUnit,
              partitionSize,
              transformType,
              transformUnit,
              transformSize,
              connectionId));
    }
    return tablePartitionSpecs;
  }

  private static class TablePartitionSpec {
    private Long dependencyId;
    private Long targetId;
    private Long tableId;
    private PartitionUnit partitionUnit = null;
    private Integer partitionSize = null;
    private PartitionTransformType transformType = null;
    private PartitionUnit transformUnit = null;
    private Integer transformSize = null;
    private Long connectionId = null;

    public TablePartitionSpec(
        Long dependencyId,
        Long targetId,
        Long tableId,
        PartitionUnit partitionUnit,
        Integer partitionSize,
        PartitionTransformType transformType,
        PartitionUnit transformUnit,
        Integer transformSize,
        Long connectionId) {
      this.dependencyId = dependencyId;
      this.targetId = targetId;
      this.tableId = tableId;
      this.partitionUnit = partitionUnit;
      this.partitionSize = partitionSize;
      this.transformType = transformType;
      this.transformUnit = transformUnit;
      this.transformSize = transformSize;
      this.connectionId = connectionId;
    }

    public Long getDependencyId() {
      return dependencyId;
    }

    public Long getTargetId() {
      return targetId;
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

    public Long getConnectionId() {
      return connectionId;
    }

    @Override
    public String toString() {
      return "TablePartitionSpec{"
          + "dependencyId="
          + dependencyId
          + ", targetId="
          + targetId
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
          + ", connectionId="
          + connectionId
          + '}';
    }
  }
}
