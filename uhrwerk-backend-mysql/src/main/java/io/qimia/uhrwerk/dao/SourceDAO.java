package io.qimia.uhrwerk.dao;


import io.qimia.uhrwerk.common.metastore.config.SourceResult;
import io.qimia.uhrwerk.common.metastore.config.SourceService;
import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import io.qimia.uhrwerk.common.model.Source;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SourceDAO implements SourceService {
    private java.sql.Connection db;

    public SourceDAO(java.sql.Connection db) {
        this.db = db;
    }

    private static final String UPSERT_SOURCE =
            "INSERT INTO SOURCE(id, table_id, connection_id, path, format, partition_unit, partition_size, sql_select_query, " +
                    "sql_partition_query, partition_column, partition_num, query_column)\n"
                    + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)\n"
                    + "ON DUPLICATE KEY UPDATE \n"
                    + "path=?, format=?, partition_unit=?, partition_size=?, sql_select_query=?, sql_partition_query=?, " +
                    "partition_column=?, partition_num=?, query_column=?";

    private static final String SELECT_BY_ID =
            "SELECT id, table_id, connection_id, path, format, partition_unit, partition_size, sql_select_query, " +
                    "sql_partition_query, partition_column, partition_num, query_column\n" +
                    "FROM SOURCE\n" +
                    "WHERE id =?";

    private void saveToDb(Source source) throws SQLException, NullPointerException {
        PreparedStatement insert = db.prepareStatement(UPSERT_SOURCE, Statement.RETURN_GENERATED_KEYS);
        // INSERT
        // ids
        insert.setLong(1, source.getId());
        insert.setLong(2, source.getTableId());

        insert.setLong(3, source.getConnection().getId());
        // other fields
        insert.setString(4, source.getPath());
        insert.setString(5, source.getFormat());
        insert.setString(6, source.getPartitionUnit().name());
        insert.setInt(7, source.getPartitionSize());
        insert.setString(8, source.getSelectQuery());
        insert.setString(9, source.getParallelLoadQuery());
        insert.setString(10, source.getParallelLoadColumn());
        insert.setInt(11, source.getParallelLoadNum());
        insert.setString(12, source.getSelectColumn());

        // UPDATE
        insert.setString(13, source.getPath());
        insert.setString(14, source.getFormat());
        insert.setString(15, source.getPartitionUnit().name());
        insert.setInt(16, source.getPartitionSize());
        insert.setString(17, source.getSelectQuery());
        insert.setString(18, source.getParallelLoadQuery());
        insert.setString(19, source.getParallelLoadColumn());
        insert.setInt(20, source.getParallelLoadNum());
        insert.setString(21, source.getSelectColumn());

        JdbcBackendUtils.singleRowUpdate(insert);
    }

    private Source getSource(PreparedStatement select) throws SQLException {
        ResultSet record = select.executeQuery();
        if (record.next()) {
            Source res = new Source();
            res.setId(record.getLong(1));
            res.setTableId(record.getLong(2));

            ConnectionDAO connectionDAO = new ConnectionDAO(db);
            Connection connection = connectionDAO.getById(record.getLong(3));
            res.setConnection(connection);

            res.setPath(record.getString(4));
            res.setFormat(record.getString(5));
            res.setPartitionUnit(PartitionUnit.valueOf(record.getString(6)));
            res.setPartitionSize(record.getInt(7));
            res.setSelectQuery(record.getString(8));
            res.setParallelLoadQuery(record.getString(9));
            res.setParallelLoadColumn(record.getString(10));
            res.setParallelLoadNum(record.getInt(11));
            res.setSelectColumn(record.getString(12));

            return res;
        }
        return null;
    }

    private Source getById(Long id) throws SQLException {
        PreparedStatement select = db.prepareStatement(SELECT_BY_ID);
        select.setLong(1, id);
        return getSource(select);
    }

    @Override
    public SourceResult save(Source source, boolean overwrite) {
        SourceResult result = new SourceResult();
        result.setNewResult(source);
        try {
            if (source.getConnection() == null) {
                throw new NullPointerException("The connection in this source is null. It needs to be set.");
            }
            if (!overwrite) {
                Source oldSource = getById(source.getId());
                if (oldSource != null && !oldSource.equals(source)) {
                    result.setOldResult(oldSource);
                    result.setMessage(
                            String.format(
                                    "A Source with id=%d and different values already exists in the Metastore.",
                                    source.getId()));
                    result.setError(true);
                    result.setSuccess(false);

                    return result;
                }
            } else {
                Source oldSource = getById(source.getId());
                if (oldSource != null
                        && (!oldSource.getId().equals(source.getId())
                        || !oldSource.getConnection().equals(source.getConnection())
                        || !oldSource.getTableId().equals(source.getTableId()))) {
                    throw new SQLException("It is not possible to overwrite these fields: id, tableId, connection");
                }
            }
            saveToDb(source);
            result.setSuccess(true);
            result.setError(false);
            result.setOldResult(source);
        } catch (SQLException | NullPointerException e) {
            result.setError(true);
            result.setSuccess(false);
            result.setException(e);
            result.setMessage(e.getMessage());
        }
        return result;
    }

    @Override
    public SourceResult[] save(Source[] sources, boolean overwrite) {
        SourceResult[] results = new SourceResult[sources.length];

        for (int i = 0; i < sources.length; i++) {
            results[i] = save(sources[i], overwrite);
        }

        return results;
    }
}
