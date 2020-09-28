package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.PartitionDependencyResult;
import io.qimia.uhrwerk.common.metastore.config.PartitionDependencyService;
import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult;
import io.qimia.uhrwerk.common.model.Partition;
import io.qimia.uhrwerk.common.model.PartitionDependencyHash;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PartitionDependencyDAO implements PartitionDependencyService {

    private static final String INSERT_PARTDEP = "INSERT INTO PARTITION_DEPENDENCY (id, partition_id, dependency_partition_id) " +
            "VALUES (?, ?, ?)";

    private static final String REMOVE_PARTDEP = "DELETE FROM PARTITION_DEPENDENCY WHERE id = ?";

    private static final String REMOVE_PARTDEP_BY_PART_ID = "DELETE FROM PARTITION_DEPENDENCY WHERE partition_id = ?";

    private static final String GET_DEP_PART_ID_BY_PART_ID = "SELECT dependency_partition_id FROM PARTITION_DEPENDENCY " +
            "WHERE partition_id = ?";

    private static final String GET_PART_ID_BY_DEP_PART_ID = "SELECT partition_id FROM PARTITION_DEPENDENCY " +
            "WHERE dependency_partition_id = ?";

    private final java.sql.Connection db;

    public PartitionDependencyDAO(java.sql.Connection db) {
        this.db = db;
    }

    private void store(long childPartitionId, DependencyResult dependency) throws SQLException {
        PreparedStatement statement = db.prepareStatement(INSERT_PARTDEP);
        for (Partition p : dependency.getPartitions()) {
            long parentId = p.getId();
            statement.setLong(1, PartitionDependencyHash.generateId(childPartitionId, parentId));
            statement.setLong(2, childPartitionId);
            statement.setLong(3, parentId);
            statement.addBatch();
        }
        statement.executeBatch();
    }

    private void storeAll(long childPartitionId, DependencyResult[] dependencies) throws SQLException {
        PreparedStatement statement = db.prepareStatement(INSERT_PARTDEP);
        for (DependencyResult dependency : dependencies) {
            for (Partition p : dependency.getPartitions()) {
                long parentId = p.getId();
                statement.setLong(1, PartitionDependencyHash.generateId(childPartitionId, parentId));
                statement.setLong(2, childPartitionId);
                statement.setLong(3, parentId);
                statement.addBatch();
            }
        }
        statement.executeBatch();
    }

    /**
     * Remove all partition-dependencies tied to child partition and all partitions part of a DependencyResult
     *
     * @param childPartitionId id of child partition
     * @param dependency       DependencyResult object with one or more partitions
     * @throws SQLException
     */
    private void removeByCombination(Long childPartitionId, DependencyResult dependency) throws SQLException {
        PreparedStatement statement = db.prepareStatement(REMOVE_PARTDEP);
        for (Partition p : dependency.getPartitions()) {
            long id = PartitionDependencyHash.generateId(childPartitionId, p.getId());
            statement.setLong(1, id);
            statement.addBatch();
        }
        statement.executeBatch();
    }

    /**
     * Remove all partition-dependencies tied to a single child partition
     *
     * @param childPartitionId id of the child partition
     * @throws SQLException
     */
    private void removeByChildId(Long childPartitionId) throws SQLException {
        PreparedStatement statement = db.prepareStatement(REMOVE_PARTDEP_BY_PART_ID);
        statement.setLong(1, childPartitionId);
        statement.executeUpdate();
    }

    /**
     * Save the one or more partition dependencies for a child partition and a parent-dependency's one or more partitions.
     * An identity-transformation would mean a single partition as parent whereas a aggregate would mean multiple
     * Warning: Does not store the Partitions themselves
     *
     * @param childPartitionId id of the child partition
     * @param dependency       DependencyResult object with information about which partitions were used for this dependency
     * @param overwrite        overwrite previously found dependencies or not
     * @return PartitionDependencyResult denoting success or what kind of error was generated
     */
    @Override
    public PartitionDependencyResult save(Long childPartitionId, DependencyResult dependency, boolean overwrite) {
        PartitionDependencyResult saveResult = new PartitionDependencyResult();
        try {
            if (overwrite) {
                removeByCombination(childPartitionId, dependency);
            }
            // WARNING: Will fail sql execution if id (meaning relation) already exists
            store(childPartitionId, dependency);
            saveResult.setSuccess(true);
            saveResult.setError(false);
        } catch (SQLException e) {
            saveResult.setSuccess(false);
            saveResult.setError(true);
            saveResult.setException(e);
            saveResult.setMessage(e.getMessage());
        }
        return saveResult;
    }

    /**
     * Remove all partition dependencies for a particular child partition and a dependency's DependencyResult
     *
     * @param childPartitionId id of child partition
     * @param dependency       DependencyResult object with information about which partitions were used for this dependency
     * @return PartitionDependencyResult denoting success or what kind of error was generated
     */
    @Override
    public PartitionDependencyResult remove(Long childPartitionId, DependencyResult dependency) {
        PartitionDependencyResult saveResult = new PartitionDependencyResult();
        try {
            removeByCombination(childPartitionId, dependency);
            saveResult.setSuccess(true);
            saveResult.setError(false);
        } catch (SQLException e) {
            saveResult.setSuccess(false);
            saveResult.setError(true);
            saveResult.setException(e);
            saveResult.setMessage(e.getMessage());
        }
        return saveResult;
    }

    /**
     * Create partition dependencies for all the dependencies of a given child partition
     * Warning: Does not store the Partitions themselves
     *
     * @param childPartitionId id of the child partition object
     * @param dependencies     Array of DependencyResult for each of the dependencies
     * @param overwrite        overwrite previously found dependencies or not
     * @return PartitionDependencyResult denoting success or what kind of error was generated
     */
    @Override
    public PartitionDependencyResult saveAll(Long childPartitionId, DependencyResult[] dependencies, boolean overwrite) {
        PartitionDependencyResult saveResult = new PartitionDependencyResult();
        try {
            if (overwrite) {
                removeByChildId(childPartitionId);
            }
            // WARNING: Will fail sql execution if id (meaning relation) already exists
            storeAll(childPartitionId, dependencies);
            saveResult.setSuccess(true);
            saveResult.setError(false);
        } catch (SQLException e) {
            saveResult.setSuccess(false);
            saveResult.setError(true);
            saveResult.setException(e);
            saveResult.setMessage(e.getMessage());
        }
        return saveResult;
    }

    /**
     * Remove all partition dependencies for a particular child partition
     *
     * @param childPartitionId id of child partition
     * @return PartitionDependencyResult denoting success or what kind of error was generated
     */
    @Override
    public PartitionDependencyResult removeAll(Long childPartitionId) {
        PartitionDependencyResult saveResult = new PartitionDependencyResult();
        try {
            removeByChildId(childPartitionId);
            saveResult.setSuccess(true);
            saveResult.setError(false);
        } catch (SQLException e) {
            saveResult.setSuccess(false);
            saveResult.setError(true);
            saveResult.setException(e);
            saveResult.setMessage(e.getMessage());
        }
        return saveResult;
    }

    /**
     * Get all partition (ids) which depend on a particular partition (id)
     *
     * @param parentId id of the
     * @return
     * @throws SQLException
     */
    protected List<Long> getChildIds(long parentId) throws SQLException {
        ArrayList<Long> childIds = new ArrayList<>();
        PreparedStatement statement = db.prepareStatement(GET_PART_ID_BY_DEP_PART_ID);
        statement.setLong(1, parentId);
        ResultSet record = statement.executeQuery();
        while (record.next()) {
            childIds.add(record.getLong("partition_id"));
        }
        return childIds;
    }

    /**
     * Get all dependency partition (ids) which are required by a partition (id)
     *
     * @param childId
     * @return
     * @throws SQLException
     */
    protected List<Long> getParentIds(long childId) throws SQLException {
        ArrayList<Long> parentIds = new ArrayList<>();
        PreparedStatement statement = db.prepareStatement(GET_DEP_PART_ID_BY_PART_ID);
        statement.setLong(1, childId);
        ResultSet record = statement.executeQuery();
        while (record.next()) {
            parentIds.add(record.getLong("dependency_partition_id"));
        }
        return parentIds;
    }

    /**
     * Retrieve all parent partition for a given (child) partition
     * @param childPartition a partition with partition dependencies
     * @return
     */
    @Override
    public List<Partition> getParentPartitions(Partition childPartition) {
        PartitionDAO partitionRetriever = new PartitionDAO(db);
        ArrayList<Partition> parentPartitions = new ArrayList<>();
        try {
            var parentIds = getParentIds(childPartition.getId());
            for (Long parentId: parentIds) {
                parentPartitions.add(partitionRetriever.getById(parentId));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return parentPartitions;
    }
}
