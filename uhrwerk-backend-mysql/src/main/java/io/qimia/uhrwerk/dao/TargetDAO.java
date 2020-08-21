package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.TargetResult;
import io.qimia.uhrwerk.common.metastore.config.TargetService;
import io.qimia.uhrwerk.common.model.Target;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;

public class TargetDAO implements TargetService {

    java.sql.Connection db;
    ConnectionDAO connectionRetriever;

    public TargetDAO(Connection db) {
        this.db = db;
        connectionRetriever = new ConnectionDAO(db);
    }

    private static final String SELECT_BY_TABLE_ID =
            "SELECT id, table_id, connection_id, format FROM TARGET WHERE table_id = ?";

    private static final String DELETE_BY_TABLE_ID = "DELETE FROM TARGET WHERE table_id = ?";

    private static final String INSERT_TARGET =
            "INSERT INTO TARGET (id, table_id, connection_id, format) "
                    + "VALUES (?, ?, ?, ?)";

    private static final String SELECT_BY_ID =
            "SELECT id, table_id, connection_id, format FROM TARGET WHERE id = ?";

    /**
     * Compare if two targets have the same format and same connection-name (which we assume to be unique)
     *
     * @return true if similar, false if not
     */
    public static boolean compareTargets(Target trueTarget, Target newTarget) {
        return (trueTarget.getFormat().equals(newTarget.getFormat()))
                && (trueTarget.getConnection().getName().equals(newTarget.getConnection().getName()));
    }

    /**
     * Compare given vs stored Targets. Assumes that both arrays are populated. For use when a save operation
     * becomes a check if anything has changed.
     *
     * @param givenTargets  targets handed to TargetDAO
     * @param storedTargets targets returned from the db
     * @return a TargetResult object like save should return
     */
    public static TargetResult compareStoredTargets(Target[] givenTargets, Target[] storedTargets) {
        var result = new TargetResult();
        HashMap<String, Target> storedTargetLookup = new HashMap<>();
        for (Target storedTarget : storedTargets) {
            storedTargetLookup.put(storedTarget.getFormat(), storedTarget);
        }
        for (Target inputTarget : givenTargets) {
            String inputTargetFormat = inputTarget.getFormat();
            Target matchingStoredTarget = storedTargetLookup.get(inputTargetFormat);
            if (matchingStoredTarget == null) {
                result.setSuccess(false);
                result.setError(false);
                result.setMessage("New target added which was not stored previously for format " + inputTargetFormat);
                return result;
            }
            // NOTE: Only checks format and tableId (nothing connection based!)
            boolean compareRes = compareTargets(matchingStoredTarget, inputTarget);
            if (!compareRes) {
                result.setSuccess(false);
                result.setError(false);
                result.setMessage("Target is not equal for format " + inputTargetFormat);
                return result;
            }
        }
        result.setSuccess(true);
        result.setError(false);
        result.setStoredTargets(storedTargets);
        return result;
    }

    /**
     * git all targets which can be found for a table-id
     * @param tableId id of table
     * @return target array or empty array if no targets are found
     * @throws SQLException can throw database query errors
     */
    private Target[] getTargetsByTable(long tableId) throws SQLException {
        ArrayList<Target> targets = new ArrayList<>();
        PreparedStatement statement = db.prepareStatement(SELECT_BY_TABLE_ID);
        statement.setLong(1, tableId);
        ResultSet record = statement.executeQuery();
        while (record.next()) {
            var target = new Target();
            target.setId(record.getLong("id"));
            target.setTableId(record.getLong("table_id"));
            var conn = connectionRetriever.getById(record.getLong("connection_id"));
            // WARNING: What if connection can't be found (should not be possible)
            target.setConnection(conn);
            target.setFormat(record.getString("format"));
            targets.add(target);
        }
        return targets.toArray(new Target[0]);
    }

    /**
     * Delete all targets for a given table. Assumes that targets can be removed (and perform no further checks)
     * @param tableId id for the table
     * @throws SQLException can throw database query errors
     */
    public void deleteTargetsByTable(long tableId) throws SQLException {
        // TODO: WARNING ASSUMES THAT REMOVING TARGETS WILL NOT BE BLOCKED BY FOREIGN-KEY
        PreparedStatement statement = db.prepareStatement(DELETE_BY_TABLE_ID);
        statement.setLong(1, tableId);
        statement.executeUpdate();
    }

    /**
     * Insert a list of targets into the Metastore
     * @param targets array of target model objects
     * @throws SQLException can throw database query errors
     */
    public void insertTargets(Target[] targets) throws SQLException {
        PreparedStatement statement = db.prepareStatement(INSERT_TARGET);
        for (Target target : targets) {
            statement.setLong(1, target.getId());
            statement.setLong(2, target.getTableId());
            var conn = target.getConnection();
            statement.setLong(3, conn.getId());
            statement.setString(4, target.getFormat());
            statement.addBatch();
        }
        statement.executeBatch();
    }

    /**
     * Save an array of targets for a particular table
     * @param targets all targets for some table
     * @param tableId id of the table
     * @param overwrite are overwrites allowed or not
     * @return result object showing what is stored, exceptions, if save was successful, and if not, why
     */
    public TargetResult save(Target[] targets, Long tableId, boolean overwrite) {
        var saveResult = new TargetResult();

        try {
            if (!overwrite) {
                Target[] storedTargets = getTargetsByTable(tableId);
                // WARNING: Assumes Format is unique per table's targets
                if (storedTargets.length > 0) {
                    return compareStoredTargets(targets, storedTargets);
                }
                // else insert all Target values found
            }

            // enrich targets
            for (Target target : targets) {
                var shellConn = target.getConnection();
                var shellConnName = shellConn.getName();
                var newTargetConn = connectionRetriever.getByName(
                        connectionRetriever.getDb(), shellConnName);
                if (newTargetConn == null) {
                    saveResult.setSuccess(false);
                    saveResult.setError(false); // todo this might be wrong?
                    saveResult.setMessage("Could not find connection " + shellConnName + " for target-format " +
                            target.getFormat());
                    return saveResult;
                }
                target.setConnection(newTargetConn);    // warning: mutates targets
            }

            if (overwrite) {
                // Delete if there are any
                deleteTargetsByTable(tableId);
            }
            // insert new targets
            insertTargets(targets);
            saveResult.setSuccess(true);
            saveResult.setError(false);
            saveResult.setStoredTargets(targets);
        } catch (SQLException e) {
            saveResult.setSuccess(false);
            saveResult.setError(true);
            saveResult.setException(e);
            saveResult.setMessage(e.getMessage());
        }
        return saveResult;
    }

    /**
     * Retrieve a target by target-id
     *
     * @param id id (can be generated by Target)
     * @return Optional with Target if found and empty in all other cases
     */
    public Optional<Target> get(Long id) {
        Optional<Target> out = Optional.empty();
        try {
            PreparedStatement select = db.prepareStatement(SELECT_BY_ID);
            select.setLong(1, id);
            ResultSet record = select.executeQuery();

            if (record.next()) {
                var outTarget = new Target();
                outTarget.setId(record.getLong("id"));
                outTarget.setTableId(record.getLong("table_id"));
                var conn = connectionRetriever.getById(record.getLong("connection_id"));
                // WARNING: What if connection can't be found (should not be possible)
                outTarget.setConnection(conn);
                outTarget.setFormat(record.getString("format"));
                out = Optional.of(outTarget);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return out;
    }

    /**
     * Retrieve all targets set for a table
     * @param tableId id of table for which the targets are defined
     * @return array of Target model objects
     */
    public Target[] getTableTargets(Long tableId) {
        Target[] targets = new Target[0];
        try {
            targets = getTargetsByTable(tableId);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return targets;
    }
}
