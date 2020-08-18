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
import java.util.ArrayList;
import java.util.List;

public class SourceDAO implements SourceService {
    private java.sql.Connection db;

    public SourceDAO() {
    }

    public SourceDAO(java.sql.Connection db) {
        this.db = db;
    }

    public java.sql.Connection getDb() {
        return db;
    }

    public void setDb(java.sql.Connection db) {
        this.db = db;
    }

    private static final String INSERT_SOURCE =
            "INSERT INTO SOURCE(id, table_id, connection_id, path, format, partition_unit, partition_size, sql_select_query, " +
                    "sql_partition_query, partition_column, partition_num, query_column)\n"
                    + "VALUES(?,?,?,?,?,?,?,?,?,?,?)";

    private static final String SELECT_BY_ID =
            "SELECT id, table_id, connection_id, path, format, partition_unit, partition_size, sql_select_query, " +
                    "sql_partition_query, partition_column, partition_num, query_column\n" +
                    "FROM SOURCE\n" +
                    "WHERE id =?";

    private Long saveToDb(Source source) throws SQLException {
        PreparedStatement insert = db.prepareStatement(INSERT_SOURCE, Statement.RETURN_GENERATED_KEYS);
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

        return JdbcBackendUtils.singleRowUpdate(insert);
    }

    private Source getSource(PreparedStatement select) throws SQLException {
        ResultSet record = select.executeQuery();
        if (record.next()) {
            Source res = new Source();
            res.setId(record.getLong(1));
            res.setTableId(record.getLong(2));

            ConnectionDAO connectionDAO = new ConnectionDAO(db);
            Connection connection = connectionDAO.getById(record.getLong(4));
            if (connection == null) {
                throw new SQLException("There is no connection for an existing source in the Metastore.");
            } else {
                res.setConnection(connection);
            }

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
    public SourceResult save(Source source, boolean overwrite, Long tableId) {
        SourceResult result = new SourceResult();
        result.setNewResult(source);
        try {
            if (!overwrite) {
                Source oldSource = getById(source.getId());
                if (oldSource != null) {
                    result.setOldResult(oldSource);
                    result.setMessage(
                            String.format(
                                    "A Source with id=%d already exists in the Metastore.",
                                    source.getId()));
                    return result;
                }
            }
            Long id = saveToDb(source);
            if (!id.equals(source.getId())) {
                throw new SQLException("The id in the Metastore is not the same as in the passed object.");
            }
            result.setSuccess(true);
            result.setOldResult(source);
        } catch (SQLException e) {
            result.setError(true);
            result.setException(e);
            result.setMessage(e.getMessage());
        }
        return result;
    }

    public List<Long> save(Source[] sources, Long tableId) throws SQLException {
        List<Long> ids = new ArrayList<>(sources.length);

        for (Source source : sources) {
            source.setTableId(tableId);
            Long id = saveToDb(source);
            ids.add(id);
        }

        return ids;
    }
}
