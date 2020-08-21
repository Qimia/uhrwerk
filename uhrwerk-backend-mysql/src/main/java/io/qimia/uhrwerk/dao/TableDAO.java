package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.TableResult;
import io.qimia.uhrwerk.common.metastore.config.TableService;
import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult;
import io.qimia.uhrwerk.common.metastore.dependency.TableDependencyService;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResult;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResultSet;
import io.qimia.uhrwerk.common.model.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;

public class TableDAO implements TableDependencyService, TableService {

    private final java.sql.Connection db;
    private final PartitionDAO partitionDAO;
    private final ConnectionDAO connectionDAO;
    private final TargetDAO targetDAO;
    private final DependencyDAO dependencyDAO;
    private final SourceDAO sourceDAO;

    private static final String SELECT_BY_ID =
            "SELECT id, area, vertical, name, partition_unit, partition_size, parallelism, max_bulk_size, version\n" +
                    "FROM TABLE_\n" +
                    "WHERE id = ?";

    public TableDAO(java.sql.Connection db) {
        this.db = db;
        this.partitionDAO = new PartitionDAO(db);
        this.connectionDAO = new ConnectionDAO(db);
        this.targetDAO = new TargetDAO(db);
        this.dependencyDAO = new DependencyDAO(db);
        this.sourceDAO = new SourceDAO(db);
    }

    private static final String INSERT =
            "INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size) "
                    + "VALUES(?,?,?,?,?,?,?,?,?)";

    private static final String DELETE_BY_ID = "DELETE FROM TABLE_ WHERE id = ?";

    @Override
    public TableResult save(Table table, boolean overwrite) {
        TableResult tableResult = new TableResult();
        tableResult.setNewResult(table);
        var tableId = table.getId();

        try {
            if (!overwrite) {
                Table oldTable = getById(table.getId());
                if (oldTable != null) {
                    tableResult.setOldResult(oldTable);

                    if (!oldTable.equals(table)) {
                        tableResult.setMessage(
                                String.format(
                                        "A Table with id=%d and different values already exists in the Metastore.",
                                        tableId));
                        tableResult.setError(true);
                    } else {
                        tableResult.setSuccess(true);
                    }

                    return tableResult;
                }
            } else {
                deleteById(tableId);
            }
            saveTable(table);
            if (table.getTargets() != null && table.getTargets().length > 0) {
                var targetResult = targetDAO.save(table.getTargets(), tableId, overwrite);
                tableResult.setTargetResult(targetResult);
            }
            if (table.getDependencies() != null && table.getDependencies().length > 0) {
                var dependencyResult = dependencyDAO
                        .save(
                                table.getDependencies(),
                                tableId,
                                table.getPartitionUnit(),
                                table.getPartitionSize(),
                                overwrite);

                tableResult.setDependencyResult(dependencyResult);
            }
            if (table.getSources() != null && table.getSources().length > 0) {
                var sourceResults = sourceDAO.save(table.getSources(), overwrite);
                tableResult.setSourceResults(sourceResults);
            }
            tableResult.setSuccess(true);
            tableResult.setOldResult(table);
        } catch (SQLException | NullPointerException e) {
            tableResult.setError(true);
            tableResult.setException(e);
            tableResult.setMessage(e.getMessage());
        }

        return tableResult;
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
        insert.setString(6, table.getPartitionUnit().name());
        insert.setInt(7, table.getPartitionSize());
        insert.setInt(8, table.getParallelism());
        insert.setInt(9, table.getMaxBulkSize());
        insert.executeUpdate();
    }

    private Table getById(Long id) throws SQLException {
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
            res.setPartitionUnit(PartitionUnit.valueOf(record.getString("partition_unit")));
            res.setPartitionSize(record.getInt("partition_size"));
            res.setParallelism(record.getInt("parallelism"));
            res.setMaxBulkSize(record.getInt("max_bulk_size"));
            res.setVersion(record.getString("version"));

            return res;
        }
        return null;
    }

    private static final String SELECT_TABLE_PARTITION_SPEC =
            "SELECT D.id,\n"
                    + "       TR.id,\n"
                    + "       D.dependency_target_id,\n"
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

    @Override
    public TablePartitionResultSet processingPartitions(
            Table table, LocalDateTime[] requestedPartitionTs) throws SQLException {
        // FIXME which target for the table should be used for getting (already) processed partition of
        // the table
        Partition[] processedPartitions =
                partitionDAO.getPartitions(table.getTargets()[0].getId(), requestedPartitionTs);
        TreeSet<LocalDateTime> processedTs = new TreeSet<>();
        for (int i = 0; i < processedPartitions.length; i++) {
            processedTs.add(processedPartitions[i].getPartitionTs());
        }

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

        DependencyResult[][] dependencyResults =
                new DependencyResult[requestedPartitionTs.length][tablePartitionSpecs.size()];
        for (int j = 0; j < tablePartitionSpecs.size(); j++) {
            TablePartitionSpec spec = tablePartitionSpecs.get(j);
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
                Partition[] depPartitions = partitionDAO.getPartitions(spec.getTargetId(), partitionTs[i]);
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
            for (int j = 0; j < results.length; j++) {
                success &= results[j].isSuccess();
                if (results[j].isSuccess()) resolvedDependencies.add(results[j]);
                else failedDependencies.add(results[j]);
            }
            if (success) {
                tablePartitionResult.setResolved(true);
                tablePartitionResult.setResolvedDependencies(results);
            } else {
                tablePartitionResult.setResolvedDependencies(
                        resolvedDependencies.toArray(new DependencyResult[resolvedDependencies.size()]));
                tablePartitionResult.setFailedDependencies(
                        failedDependencies.toArray(new DependencyResult[failedDependencies.size()]));
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
        tablePartitionResultSet.setProcessed(
                processed.toArray(new TablePartitionResult[processed.size()]));
        tablePartitionResultSet.setResolved(
                resolved.toArray(new TablePartitionResult[resolved.size()]));
        tablePartitionResultSet.setFailed(failed.toArray(new TablePartitionResult[failed.size()]));

        tablePartitionResultSet.setProcessedTs(
                processedTs.toArray(new LocalDateTime[processedTs.size()]));
        tablePartitionResultSet.setResolvedTs(resolvedTs.toArray(new LocalDateTime[resolvedTs.size()]));
        tablePartitionResultSet.setFailedTs(failedTs.toArray(new LocalDateTime[failedTs.size()]));
        return tablePartitionResultSet;
    }

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
            PartitionUnit partitionUnit = PartitionUnit.valueOf(resultSet.getString(4));
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
