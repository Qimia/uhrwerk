package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.DependencyStoreResult;
import io.qimia.uhrwerk.common.metastore.config.DependencyStoreService;
import io.qimia.uhrwerk.common.model.Dependency;
import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.common.model.PartitionUnit;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class DependencyDAO implements DependencyStoreService {

    private static final String CHECK_TABLE_N_TARGET = "SELECT tab.id, tab.name, tab.partition_unit, tab.partition_size" +
            " FROM TARGET tar JOIN TABLE_ tab ON tar.table_id = tab.id WHERE tar.id IN (%s)";

    private static String checkTableNTargetQuery (Long[] targetIds) {
        var concatIds = Arrays.toString(targetIds);
        return String.format(CHECK_TABLE_N_TARGET, concatIds.substring(1, concatIds.length() - 1));
    }

    private static final String DELETE_DEPENDENCIES = "DELETE FROM DEPENDENCY WHERE table_id = %d";

    private static String deleteDependenciesQuery(long tableId) {
        return String.format(DELETE_DEPENDENCIES, tableId);
    }

    private static final String INSERT_DEPENDENCY_IDENTITY = "INSERT INTO DEPENDENCY " +
            "(id, table_id, target_id, transform_type) VALUES (%d, %d, %d, 'identity')";

    private static final String INSERT_DEPENDENCY_VIRTUAL = "INSERT INTO DEPENDENCY " +
            "(id, table_id, target_id, transform_type, transform_partition_size) VALUES (%d, %d, %d, '%s', %d)";

    private static String insertDependencyQuery(Dependency d) {
        if (d.getTransformType() == PartitionTransformType.IDENTITY) {
            return String.format(INSERT_DEPENDENCY_IDENTITY, d.getId(), d.getTableId(), d.getTargetId());
        } else {
            return String.format(INSERT_DEPENDENCY_VIRTUAL, d.getId(), d.getTableId(), d.getTargetId(),
                    d.getTransformType().toString(), d.getTransformPartitionSize());
        }
    }

    private java.sql.Connection db;

    public DependencyDAO(java.sql.Connection db) {
        this.db = db;
    }

    class ExistingDepResult {
        boolean found;
        boolean correct;
    }

    public ExistingDepResult checkExistingDependencies(Dependency[] dependencies) {
        var res = new ExistingDepResult();
        res.found = false;
        res.correct = false;
        return res;
    }

    class TableRes {
        public Long tableId;
        public PartitionUnit partitionUnit ;
        public int partitionSize;
    }

    class FindQueryResult {
        List<TableRes> foundTables;
        Set<String> missingNames;
    }

    public FindQueryResult findTables(Dependency[] dependencies) throws SQLException, IllegalArgumentException {
        Long[] targetIds = Arrays.stream(dependencies).map(Dependency::getTargetId).toArray(Long[]::new);
        Set<String> namesToFind = Arrays.stream(dependencies).map(Dependency::getTableName).collect(Collectors.toSet());

        // TODO: Are tablenames unique in the context of a single table's dependencies
        // now assumes yes but in theory could be no -> then we need to map the missing target-ids back to full table info

        Statement statement = db.createStatement();
        String query = DependencyDAO.checkTableNTargetQuery(targetIds);
        ResultSet rs = statement.executeQuery(query);

        ArrayList<TableRes> foundTables = new ArrayList<>(dependencies.length);
        while (rs.next()) {
            var singleRes = new TableRes();
            singleRes.tableId = rs.getLong("tab.id");
            singleRes.partitionUnit = PartitionUnit.valueOf(rs.getString("tab.partition_unit").toUpperCase());
            singleRes.partitionSize = rs.getInt("tab.partition_size");
            namesToFind.remove(rs.getString("tab.name"));
            foundTables.add(singleRes);
        }

        FindQueryResult results = new FindQueryResult();
        results.foundTables = foundTables;
        results.missingNames = namesToFind;
        return results;
    }

    public DependencyStoreResult checkTablePartitionSizes(List<TableRes> dependencyTables, PartitionUnit partitionUnit, int partitionSize) {
        return null;
    }

    public void deleteAllDependencies(Long tableId) throws SQLException {
        Statement statement = db.createStatement();
        String query = DependencyDAO.deleteDependenciesQuery(tableId);
        statement.executeUpdate(query);
    }

    public void insertDependencies(Dependency[] dependencies) throws SQLException {
        Statement statement = db.createStatement();
        for (Dependency dependency : dependencies) {
            String insertStr = insertDependencyQuery(dependency);
            statement.addBatch(insertStr);
        }
        statement.executeBatch();
    }

    @Override
    public DependencyStoreResult save(
            Dependency[] dependencies,
            Long tableId,
            PartitionUnit partitionUnit,
            int partitionSize,
            boolean overwrite
    ) {
        DependencyStoreResult result = new DependencyStoreResult();
        if (!overwrite) {
            /* If not overwriting we check if the dependencies are there.
           If they are all there than all is fine. If none of them are there
           we need to write them all (at a first production run for instance).
           (which means check the dependencySizes = overwrite flow)
           If some are there and some not, then something has changed and
           the process aborts.
         */
            ExistingDepResult checkDeps = checkExistingDependencies(dependencies);
        }

        // overwrite or write needed -> first get all tables (abort if table not found)
        FindQueryResult tableSearchRes = null;
        try {
            tableSearchRes = findTables(dependencies);
        } catch (Exception e) {
            result.setError(true);
            result.setException(e);
            result.setMessage(e.getMessage());
            return result;
        }
        if (!tableSearchRes.missingNames.isEmpty()) {
            result.setSuccess(false);
            result.setMessage("Missing tables: " + tableSearchRes.missingNames.toString());
        }

        // Check dependency sizes (abort if size does not match)

        // Delete all old dependencies and create new list
        try {
            deleteAllDependencies(tableId);
            insertDependencies(dependencies);
            result.setSuccess(true);
            result.setError(false);
            result.setDependenciesSaved(dependencies);
        } catch (Exception e) {
            result.setError(true);
            result.setException(e);
            result.setMessage(e.getMessage());
        }
        return result;
    }
}
