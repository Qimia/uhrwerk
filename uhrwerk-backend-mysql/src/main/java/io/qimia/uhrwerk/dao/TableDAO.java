package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.TableResult;
import io.qimia.uhrwerk.common.metastore.config.TableService;
import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult;
import io.qimia.uhrwerk.common.metastore.dependency.TableDependencyService;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResult;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResultSet;
import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.*;
import org.apache.log4j.Logger;

import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class TableDAO implements TableDependencyService, TableService {

  private final java.sql.Connection db;
  private final PartitionDAO partitionDAO;
  private final ConnectionDAO connectionDAO;
  private final TargetDAO targetDAO;
  private final DependencyDAO dependencyDAO;
  private final SourceDAO sourceDAO;
  private final Logger logger;

  private static final String SELECT_BY_ID =
          "SELECT id, area, vertical, name, partition_unit, partition_size, parallelism, max_bulk_size, version, partitioned,\n"
          + "class_name FROM TABLE_\n"
          + "WHERE id = ?";

  public TableDAO(java.sql.Connection db) {
    this.db = db;
    this.partitionDAO = new PartitionDAO(db);
    this.connectionDAO = new ConnectionDAO(db);
    this.targetDAO = new TargetDAO(db);
    this.dependencyDAO = new DependencyDAO(db);
    this.sourceDAO = new SourceDAO(db);
    this.logger = Logger.getLogger(this.getClass());
  }

  private static final String INSERT =
          "INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size, partitioned, "
                  + "class_name) VALUES(?,?,?,?,?,?,?,?,?,?,?)";

  private static final String DELETE_BY_ID = "DELETE FROM TABLE_ WHERE id = ?";

  @Override
  public TableResult save(Table table, boolean overwrite) {
    TableResult tableResult = new TableResult();
    tableResult.setSuccess(true);
    tableResult.setError(false);
    tableResult.setNewResult(table);
    var tableId = table.getId();

    try {
      if (!overwrite) {
        saveTablesArrays(table, tableResult, overwrite);
        Table oldTable = getById(tableId);
        if (oldTable != null) {
          tableResult.setOldResult(oldTable);

          if (!oldTable.equals(table)) {
            var message =
                String.format(
                    "A Table with id=%d and different values already exists in the Metastore.\n\n"
                        + "Passed Table:\n%s\n\n"
                        + "Table in the Metastore:\n%s",
                    tableId,
                    table.toString(),
                    oldTable.toString()); // todo improve finding differences
            tableResult.setMessage(message);
            tableResult.setSuccess(false);
          }
          return tableResult;
        }
        saveTable(table);
      } else {
        deleteById(tableId);
        saveTablesArrays(table, tableResult, overwrite);
        saveTable(table);
      }
    } catch (SQLException | NullPointerException e) {
      tableResult.setError(true);
      tableResult.setSuccess(false);
      tableResult.setException(e);
      tableResult.setMessage(e.getMessage());
    }

    return tableResult;
  }

  private void saveTablesArrays(Table table, TableResult tableResult, Boolean overwrite) {
    if (table.getTargets() != null && table.getTargets().length > 0) {
      var targetResult = targetDAO.save(table.getTargets(), table.getId(), overwrite);
      tableResult.setTargetResult(targetResult);
      if (targetResult.isSuccess()) {
        table.setTargets(targetResult.getStoredTargets());
      } else {
        tableResult.setSuccess(false);
        return;
      }
    }
    if (table.getDependencies() != null && table.getDependencies().length > 0) {
      var dependencyResult = dependencyDAO.save(table, overwrite);

      tableResult.setDependencyResult(dependencyResult);
      if (dependencyResult.isSuccess()) {
        table.setDependencies(dependencyResult.getDependenciesSaved());
      } else {
        tableResult.setSuccess(false);
        return;
      }
    }
    if (table.getSources() != null && table.getSources().length > 0) {
      var sourceResults = sourceDAO.save(table.getSources(), table, overwrite);
      tableResult.setSourceResults(sourceResults);
      for (int i = 0; i < sourceResults.length; i++) {
        if (sourceResults[i].isSuccess()) {
          table.getSources()[i] = sourceResults[i].getNewResult();
        } else {
          tableResult.setSuccess(false);
          return;
        }
      }
    }
  }

  private void deleteById(Long tableId) throws SQLException {
    targetDAO.deleteTargetsByTable(tableId);
    dependencyDAO.deleteAllDependencies(tableId);
    sourceDAO.deleteSourcesByTableId(tableId);

    PreparedStatement statement = db.prepareStatement(DELETE_BY_ID);
    statement.setLong(1, tableId);
    statement.executeUpdate();
  }

  private void saveTable(Table table) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
    insert.setLong(1, table.getId());
    insert.setString(2, table.getArea());
    insert.setString(3, table.getVertical());
    insert.setString(4, table.getName());
    insert.setString(5, table.getVersion());
    var partitionUnit = table.getPartitionUnit();
    if (partitionUnit == null) {
      insert.setNull(6, Types.VARCHAR);
    } else {
      insert.setString(6, partitionUnit.name());
    }
    insert.setInt(7, table.getPartitionSize());
    insert.setInt(8, table.getParallelism());
    insert.setInt(9, table.getMaxBulkSize());
    insert.setBoolean(10, table.isPartitioned());
    insert.setString(11, table.getClassName());
    insert.executeUpdate();
  }

  private Table getById(Long id) throws SQLException, NullPointerException {
    PreparedStatement select = db.prepareStatement(SELECT_BY_ID);
    select.setLong(1, id);
    Table table = getTable(select);

    if (table != null) {
      table.setDependencies(dependencyDAO.get(id));
      table.setTargets(targetDAO.getTableTargets(id));
      table.setSources(sourceDAO.getSourcesByTableId(id));
    }

    return table;
  }

  private Table getTable(PreparedStatement select) throws SQLException {
    ResultSet record = select.executeQuery();
    if (record.next()) {
      Table res = new Table();
      res.setId(record.getLong("id"));
      res.setArea(record.getString("area"));
      res.setVertical(record.getString("vertical"));
      res.setName(record.getString("name"));
      var partitionUnit = record.getString("partition_unit");
      if ((partitionUnit != null) && (!partitionUnit.equals(""))) {
        res.setPartitionUnit(PartitionUnit.valueOf(partitionUnit));
      }
      res.setPartitionSize(record.getInt("partition_size"));
      res.setParallelism(record.getInt("parallelism"));
      res.setMaxBulkSize(record.getInt("max_bulk_size"));
      res.setVersion(record.getString("version"));
      res.setPartitioned(record.getBoolean("partitioned"));
      res.setClassName(record.getString("class_name"));

      return res;
    }
    return null;
  }

  /**
   * Adapted version of {@link #processingPartitions processingPartitions} When a table does not
   * have a partitioning-scheme itself, this checks if itself has been processed and if not, if the
   * dependencies have been processed (Current version checks if it has been processed at any time)
   *
   * @param table table which needs to be processed
   * @return same TablePartitionResultSet but with a single reference to failed or succeeded result
   */
  private TablePartitionResultSet processPartitionlessTable(Table table, LocalDateTime requestTime) throws SQLException {
    assert (!table.isPartitioned()) : "Table can't be partitioned for partitionless processing";
    var singleResult = new TablePartitionResult();
    var resultSet = new TablePartitionResultSet();
    // FIXME checks only the first target of the table (see FIXME normal processingPartitions)
    Partition processedPartition =
            partitionDAO.getLatestPartition(table.getTargets()[0].getId());
    if (processedPartition != null) {
      // If partition found -> check if it has been processed in the last 1 minute

      Duration requestDiff = Duration.between(processedPartition.getPartitionTs(), requestTime);
      if (requestDiff.toSeconds() < 60L) {
        // Warning !! (Does **not** add dependency and resolution info)
        singleResult.setProcessed(true);
        singleResult.setPartitionTs(processedPartition.getPartitionTs());
        resultSet.setProcessed(new TablePartitionResult[] {singleResult});
        resultSet.setProcessedTs(new LocalDateTime[] {processedPartition.getPartitionTs()});
        return resultSet;
      }
      // If not last-1-minute than continue as if none were found
    }
    singleResult.setProcessed(false);

    // If there are no dependencies then this table is resolved and ready to run
    if ((table.getDependencies() == null) || (table.getDependencies().length == 0)) {
      singleResult.setResolved(true);
      singleResult.setPartitionTs(requestTime);
      singleResult.setResolvedDependencies(new DependencyResult[0]);
      singleResult.setFailedDependencies(new DependencyResult[0]);
      resultSet.setResolved(new TablePartitionResult[] {singleResult});
      resultSet.setResolvedTs(new LocalDateTime[] {requestTime});
      return resultSet;
    }

    List<TablePartitionSpec> tablePartitionSpecs = getTablePartitionSpecs(table);
    Connection[] connections = this.connectionDAO.getTableDependenciesConnections(table.getId());
    Map<Long, Connection> connectionsMap =
        Arrays.stream(connections).collect(Collectors.toMap(Connection::getId, x -> x));
    Map<Long, Dependency> dependenciesMap =
        Arrays.stream(table.getDependencies()).collect(Collectors.toMap(Dependency::getId, x -> x));

    // Go over each dependency (+ spec) and check if it's there or not
    ArrayList<DependencyResult> resolvedDependencies = new ArrayList<>();
    ArrayList<DependencyResult> failedDependencies = new ArrayList<>();
    boolean singleSuccess = true;
    for (int j = 0; j < tablePartitionSpecs.size(); j++) {
      var spec = tablePartitionSpecs.get(j);
      assert (spec.transformType.equals(PartitionTransformType.NONE))
          : "Can't have partitioned dependencies for tables without partitioning";

      DependencyResult dependencyResult = new DependencyResult();
      dependencyResult.setConnection(connectionsMap.get(spec.connectionId));
      dependencyResult.setDependency(dependenciesMap.get(spec.dependencyId));

      Partition depPartition = partitionDAO.getLatestPartition(spec.targetId);
      if (depPartition == null) {
        dependencyResult.setSuccess(false);
        dependencyResult.setFailed(new LocalDateTime[] {requestTime});
        failedDependencies.add(dependencyResult);
        singleSuccess = false;
      } else {
        dependencyResult.setSuccess(true);
        dependencyResult.setSucceeded(new LocalDateTime[] {depPartition.getPartitionTs()});
        dependencyResult.setPartitionTs(depPartition.getPartitionTs());
        dependencyResult.setPartitions(new Partition[] {depPartition});
        resolvedDependencies.add(dependencyResult);
      }
    }
    singleResult.setResolvedDependencies(resolvedDependencies.toArray(new DependencyResult[0]));
    singleResult.setFailedDependencies(failedDependencies.toArray(new DependencyResult[0]));
    singleResult.setResolved(singleSuccess);
    singleResult.setPartitionTs(requestTime);
    if (singleSuccess) {
      resultSet.setResolved(new TablePartitionResult[] {singleResult});
      resultSet.setResolvedTs(new LocalDateTime[] {requestTime});
    } else {
      resultSet.setFailed(new TablePartitionResult[] {singleResult});
      resultSet.setFailedTs(new LocalDateTime[] {requestTime});
    }
    return resultSet;
  }

  /**
   * Adapted version of {@link #processingPartitions processingPartitions} When a table has no
   * dependencies, all we need to do is check if current table has already been processed for each
   * requested partitionTS
   *
   * @param table table which needs to be processed
   * @param requestedPartitionTs list of partition starting timestamps
   * @return all info required for running a table
   * @throws SQLException
   */
  private TablePartitionResultSet processDepLessPartitions(
      Table table, LocalDateTime[] requestedPartitionTs) {
    Partition[] processedPartitions =
        partitionDAO.getPartitions(table.getTargets()[0].getId(), requestedPartitionTs);
    TreeSet<LocalDateTime> processedTs = new TreeSet<>();
    for (Partition processedPartition : processedPartitions) {
      processedTs.add(processedPartition.getPartitionTs());
    }

    List<LocalDateTime> resolvedTs = new ArrayList<>();

    List<TablePartitionResult> resolved = new ArrayList<>();
    List<TablePartitionResult> processed = new ArrayList<>();

    for (LocalDateTime requestedPartitionT : requestedPartitionTs) {
      TablePartitionResult tablePartitionResult = new TablePartitionResult();
      LocalDateTime partitionTs = requestedPartitionT;
      tablePartitionResult.setPartitionTs(partitionTs);
      tablePartitionResult.setResolved(true);
      tablePartitionResult.setFailedDependencies(new DependencyResult[0]);
      tablePartitionResult.setResolvedDependencies(new DependencyResult[0]);
      if (processedTs.contains(partitionTs)) {
        tablePartitionResult.setProcessed(true);
        processed.add(tablePartitionResult);
      } else {
        tablePartitionResult.setProcessed(false);
        resolved.add(tablePartitionResult);
        resolvedTs.add(tablePartitionResult.getPartitionTs());
      }
    }

    TablePartitionResultSet tablePartitionResultSet = new TablePartitionResultSet();
    tablePartitionResultSet.setProcessed(
        processed.toArray(new TablePartitionResult[processed.size()]));
    tablePartitionResultSet.setResolved(
        resolved.toArray(new TablePartitionResult[resolved.size()]));
    tablePartitionResultSet.setFailed(new TablePartitionResult[0]);

    tablePartitionResultSet.setProcessedTs(
        processedTs.toArray(new LocalDateTime[processedTs.size()]));
    tablePartitionResultSet.setResolvedTs(resolvedTs.toArray(new LocalDateTime[resolvedTs.size()]));
    tablePartitionResultSet.setFailedTs(new LocalDateTime[0]);
    return tablePartitionResultSet;
  }

  /**
   * Find out which partitions have already been processed for the current table, which are ready to
   * be processed and which ones can't be processed yet. This check is being done on a list of given
   * partitions which the caller want to check.
   *
   * @param table a Table which needs to be processed / produced
   * @param requestedPartitionTs which partitions need to be processed for this table
   * @return TablePartitionResultSet containing all info required for the processing and failure
   *     reporting
   * @throws SQLException
   */
  @Override
  public TablePartitionResultSet processingPartitions(
      Table table, LocalDateTime[] requestedPartitionTs) throws SQLException {
    if (!table.isPartitioned()) {
        return processPartitionlessTable(table, requestedPartitionTs[0]);
    }
    if ((table.getDependencies() == null) || (table.getDependencies().length == 0)) {
      return processDepLessPartitions(table, requestedPartitionTs);
    }

    // Check what partitions have already been processed
    // FIXME which target for the table should be used for getting (already) processed partition of
    // the table
    Partition[] processedPartitions =
        partitionDAO.getPartitions(table.getTargets()[0].getId(), requestedPartitionTs);
    TreeSet<LocalDateTime> processedTs = new TreeSet<>();
    for (int i = 0; i < processedPartitions.length; i++) {
      processedTs.add(processedPartitions[i].getPartitionTs());
    }

    // Get full spec-objects for each of the dependencies + store all connections
    List<TablePartitionSpec> tablePartitionSpecs = getTablePartitionSpecs(table);
    Connection[] connections = this.connectionDAO.getTableDependenciesConnections(table.getId());
    Map<Long, Connection> connectionsMap = new HashMap<>();
    for (int i = 0; i < connections.length; i++) {
      connectionsMap.put(connections[i].getId(), connections[i]);
    }

    Dependency[] tableDependencies = table.getDependencies();
    if (tablePartitionSpecs.size() != tableDependencies.length) {
      logger.error("Could not find all specifications for all dependencies in metastore");
      return buildResultSet(new LocalDateTime[0], new DependencyResult[0][0], new TreeSet<>());
    }
    Map<Long, Dependency> dependenciesMap = new HashMap<>();
    for (int i = 0; i < tableDependencies.length; i++) {
      dependenciesMap.put(tableDependencies[i].getId(), tableDependencies[i]);
    }

    DependencyResult[][] dependencyResults =
        new DependencyResult[requestedPartitionTs.length][tablePartitionSpecs.size()];
    for (int j = 0; j < tablePartitionSpecs.size(); j++) {
      TablePartitionSpec spec = tablePartitionSpecs.get(j);
      if (spec.transformType.equals(PartitionTransformType.NONE)) {
        // If not partitioned then check single

        Partition depPartition = partitionDAO.getLatestPartition(spec.getTargetId());
        if (depPartition == null) {
          // If there is nothing, set all to unsuccessful
          DependencyResult dependencyResult = new DependencyResult();
          dependencyResult.setConnection(connectionsMap.get(spec.connectionId));
          dependencyResult.setDependency(dependenciesMap.get(spec.dependencyId));
          dependencyResult.setSuccess(false);
          dependencyResult.setFailed(new LocalDateTime[] {});
          for (int i = 0; i < requestedPartitionTs.length; i++) {
            dependencyResults[i][j] = dependencyResult;
          }
        } else {
          // If there is something, check for every requested partition ->
          // Is the found dependency-partition's startTS equal or after the endpoint of that partition
          LocalDateTime depPartitionSnapshotTime = depPartition.getPartitionTs();
          ChronoUnit tableChronoUnit = ChronoUnit.valueOf(table.getPartitionUnit().name());
          Duration tableDuration = Duration.of(table.getPartitionSize(), tableChronoUnit);
          for (int i = 0; i < requestedPartitionTs.length; i++) {
            DependencyResult dependencyResult = new DependencyResult();
            dependencyResult.setConnection(connectionsMap.get(spec.connectionId));
            dependencyResult.setDependency(dependenciesMap.get(spec.dependencyId));

            LocalDateTime requestedPartitionEndTs = requestedPartitionTs[i].plus(tableDuration);
            if (depPartitionSnapshotTime.isEqual(requestedPartitionEndTs) || depPartitionSnapshotTime.isAfter(requestedPartitionEndTs)) {
              dependencyResult.setSuccess(true);
              dependencyResult.setSucceeded(new LocalDateTime[] {depPartition.getPartitionTs()});
              dependencyResult.setPartitionTs(depPartitionSnapshotTime);
              dependencyResult.setPartitions(new Partition[] {depPartition});
            } else {
              dependencyResult.setSuccess(false);
              dependencyResult.setFailed(new LocalDateTime[] {depPartitionSnapshotTime});
              dependencyResult.setPartitionTs(depPartitionSnapshotTime);
              dependencyResult.setPartitions(new Partition[] {depPartition});
            }
            dependencyResults[i][j] = dependencyResult;
          }
        }
      } else {
        // partitioned so first create list of which partitions to check
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
          // each partition spec check every partition
          Partition[] depPartitions =
              partitionDAO.getPartitions(spec.getTargetId(), partitionTs[i]);
          DependencyResult dependencyResult = new DependencyResult();
          dependencyResult.setConnection(connectionsMap.get(spec.connectionId));
          dependencyResult.setDependency(dependenciesMap.get(spec.dependencyId));
          if (depPartitions != null && depPartitions.length == spec.getTransformSize()) {
            dependencyResult.setSuccess(true);
            dependencyResult.setSucceeded(partitionTs[i]);
            dependencyResult.setPartitions(depPartitions);
          } else {
            List<LocalDateTime> succeeded = new ArrayList<>();
            if (depPartitions != null && depPartitions.length > 0) {
              dependencyResult.setPartitions(depPartitions);
              for (Partition partition : depPartitions) {
                LocalDateTime ts = partition.getPartitionTs();
                succeeded.add(ts);
              }
            }
            TreeSet<LocalDateTime> failed = new TreeSet<>(Arrays.asList(partitionTs[i]));
            failed.removeAll(succeeded);
            dependencyResult.setSuccess(false);
            dependencyResult.setFailed(failed.toArray(new LocalDateTime[failed.size()]));
            dependencyResult.setSucceeded(succeeded.toArray(new LocalDateTime[succeeded.size()]));
          }
          dependencyResults[i][j] = dependencyResult;
        }
      }
    }
    return buildResultSet(requestedPartitionTs, dependencyResults, processedTs);
  }

  /**
   * Build a TablePartitionResultSet out of DependencyResults and processed timestamps
   *
   * @param requestedPartitionTs Originally requested partition timestamps for processing
   * @param dependencyResults DependencyResults size [# requested partitions][# of dependencies]
   * @param processedTs Which timestamps have already been
   * @return
   */
  public static TablePartitionResultSet buildResultSet(
      LocalDateTime[] requestedPartitionTs,
      DependencyResult[][] dependencyResults,
      Set<LocalDateTime> processedTs) {
    List<LocalDateTime> resolvedTs = new ArrayList<>();
    List<LocalDateTime> failedTs = new ArrayList<>();

    List<TablePartitionResult> resolved = new ArrayList<>();
    List<TablePartitionResult> processed = new ArrayList<>();
    List<TablePartitionResult> failed = new ArrayList<>();

    for (int i = 0; i < requestedPartitionTs.length; i++) {
      TablePartitionResult tablePartitionResult = new TablePartitionResult();
      LocalDateTime partitionTs = requestedPartitionTs[i];
      tablePartitionResult.setPartitionTs(partitionTs);
      DependencyResult[] results = dependencyResults[i];
      boolean success = true;
      List<DependencyResult> resolvedDependencies = new ArrayList<>();
      List<DependencyResult> failedDependencies = new ArrayList<>();
      for (DependencyResult result : results) {
        success &= result.isSuccess();
        if (result.isSuccess()) resolvedDependencies.add(result);
        else failedDependencies.add(result);
      }
      if (success) {
        tablePartitionResult.setResolved(true);
        tablePartitionResult.setResolvedDependencies(results);
      } else {
        tablePartitionResult.setResolvedDependencies(
            resolvedDependencies.toArray(new DependencyResult[0]));
        tablePartitionResult.setFailedDependencies(
            failedDependencies.toArray(new DependencyResult[0]));
      }
      if (processedTs.contains(partitionTs)) {
        tablePartitionResult.setProcessed(true);
        processed.add(tablePartitionResult);
      } else if (tablePartitionResult.isResolved()) {
        resolved.add(tablePartitionResult);
        resolvedTs.add(tablePartitionResult.getPartitionTs());
      } else {
        failed.add(tablePartitionResult);
        failedTs.add(tablePartitionResult.getPartitionTs());
      }
    }
    TablePartitionResultSet tablePartitionResultSet = new TablePartitionResultSet();
    tablePartitionResultSet.setProcessed(processed.toArray(new TablePartitionResult[0]));
    tablePartitionResultSet.setResolved(resolved.toArray(new TablePartitionResult[0]));
    tablePartitionResultSet.setFailed(failed.toArray(new TablePartitionResult[failed.size()]));

    tablePartitionResultSet.setProcessedTs(processedTs.toArray(new LocalDateTime[0]));
    tablePartitionResultSet.setResolvedTs(resolvedTs.toArray(new LocalDateTime[0]));
    tablePartitionResultSet.setFailedTs(failedTs.toArray(new LocalDateTime[0]));
    return tablePartitionResultSet;
  }

  private static final String SELECT_TABLE_PARTITION_SPEC =
      "SELECT D.id,\n"
          + "       TR.id,\n"
          + "       T.id,\n"
          + "       T.partition_unit,\n"
          + "       T.partition_size,\n"
          + "       D.transform_type,\n"
          + "       D.transform_partition_unit,\n"
          + "       D.transform_partition_size,\n"
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

  /**
   * Find the specifications for each of the dependencies for a table
   *
   * @param table child-table for which the specifications need to be found
   * @return List of specifications containing partitioning + transform info per dependency('s
   *     table)
   * @throws SQLException
   */
  private List<TablePartitionSpec> getTablePartitionSpecs(Table table) throws SQLException {
    PreparedStatement partitionSpecs =
        db.prepareStatement(SELECT_TABLE_PARTITION_SPEC, Statement.RETURN_GENERATED_KEYS);
    partitionSpecs.setLong(1, table.getId());
    List<TablePartitionSpec> tablePartitionSpecs = new ArrayList<>();
    ResultSet resultSet = partitionSpecs.executeQuery();
    while (resultSet.next()) {
      long dependencyId = resultSet.getLong(1);
      long targetId = resultSet.getLong(2);
      long tableId = resultSet.getLong(3);
      String specTablePartitionUnit = resultSet.getString(4);
      PartitionUnit partitionUnit = null;
      if (specTablePartitionUnit != null) {
        partitionUnit = PartitionUnit.valueOf(specTablePartitionUnit);
      }
      int partitionSize = resultSet.getInt(5);
      PartitionTransformType transformType = PartitionTransformType.valueOf(resultSet.getString(6));
      String transformUnitStr = resultSet.getString(7);
      PartitionUnit transformUnit = null;
      if (transformUnitStr != null && !transformUnitStr.equals("")) {
        transformUnit = PartitionUnit.valueOf(transformUnitStr);
      }
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
