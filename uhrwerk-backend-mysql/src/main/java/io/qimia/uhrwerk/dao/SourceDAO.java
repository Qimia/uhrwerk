package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.SourceResult;
import io.qimia.uhrwerk.common.metastore.config.SourceService;
import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import io.qimia.uhrwerk.common.model.Source;
import io.qimia.uhrwerk.common.model.Table;

import java.sql.*;
import java.util.ArrayList;

public class SourceDAO implements SourceService {
  private final java.sql.Connection db;
  private final ConnectionDAO connectionDAO;

  public SourceDAO(java.sql.Connection db) {
    this.db = db;
    this.connectionDAO = new ConnectionDAO(db);
  }

  private static final String INSERT_SOURCE =
          "INSERT INTO SOURCE(id, table_id, connection_id, path, format, partition_unit, partition_size, sql_select_query, "
                  + "sql_partition_query, partition_column, partition_num, query_column, partitioned, autoloading)\n"
                  + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

  private static final String SELECT_BY_ID =
          "SELECT id, table_id, connection_id, path, format, partition_unit, partition_size, sql_select_query, "
                  + "sql_partition_query, partition_column, partition_num, query_column, partitioned, autoloading\n"
                  + "FROM SOURCE\n"
                  + "WHERE id = ?";

  private static final String SELECT_BY_TABLE_ID =
          "SELECT id, table_id, connection_id, path, format, partition_unit, partition_size, sql_select_query, "
                  + "sql_partition_query, partition_column, partition_num, query_column, partitioned, autoloading\n"
                  + "FROM SOURCE\n"
                  + "WHERE table_id = ?";

  private static final String DELETE_BY_ID = "DELETE FROM SOURCE WHERE id = ?";

  private static final String DELETE_BY_TABLE_ID = "DELETE FROM SOURCE WHERE table_id = ?";

  /**
   * Saves the source to the Metastore, doesn't do any checking anymore.
   *
   * @param source Source to save.
   * @throws SQLException         Thrown when something went wrong with the saving. This is processed by the
   *                              caller of this function.
   * @throws NullPointerException Thrown probably only when source.connection or
   *                              source.partitionUnit are null. Caught and processed again by the caller of this function.
   */
  private void saveToDb(Source source) throws SQLException, NullPointerException {
    PreparedStatement insert = db.prepareStatement(INSERT_SOURCE, Statement.RETURN_GENERATED_KEYS);
    // INSERT
    // ids
    insert.setLong(1, source.getId());
    insert.setLong(2, source.getTableId());

    insert.setLong(3, source.getConnection().getId());
    // other fields
    insert.setString(4, source.getPath());
    insert.setString(5, source.getFormat());
    PartitionUnit sourceUnit = source.getPartitionUnit();
    if (sourceUnit != null) {
      insert.setString(6, sourceUnit.name());
    } else {
      insert.setNull(6, Types.VARCHAR);
    }
    insert.setInt(7, source.getPartitionSize());
    insert.setString(8, source.getSelectQuery());
    insert.setString(9, source.getParallelLoadQuery());
    insert.setString(10, source.getParallelLoadColumn());
    insert.setInt(11, source.getParallelLoadNum());
    insert.setString(12, source.getSelectColumn());
    insert.setBoolean(13, source.isPartitioned());
    insert.setBoolean(14, source.isAutoloading());

    JdbcBackendUtils.singleRowUpdate(insert);
  }

  /**
   * Selects a source from a prepared statement and fills the result into a new Source object. Tries
   * to fill the source's connection as well.
   *
   * @param record ResultSet.
   * @return Found source or null otherwise.
   * @throws SQLException         When something goes wrong with the SQL command.
   * @throws NullPointerException When the source's connection is missing.
   */
  private Source getSource(ResultSet record) throws SQLException, NullPointerException {
    if (record != null) {
      Source res = new Source();
      res.setId(record.getLong(1));
      res.setTableId(record.getLong(2));

      Connection connection = connectionDAO.getById(record.getLong(3));
      if (connection == null) {
        throw new NullPointerException(
                "The connection for this source "
                        + "(id = "
                        + res.getId()
                        + ") is missing in the Metastore.");
      }
      res.setConnection(connection);

      res.setPath(record.getString(4));
      res.setFormat(record.getString(5));
      String sourcePartitionUnit = record.getString(6);
      if ((sourcePartitionUnit != null) && (!sourcePartitionUnit.equals(""))) {
        res.setPartitionUnit(PartitionUnit.valueOf(sourcePartitionUnit));
      }
      res.setPartitionSize(record.getInt(7));
      res.setSelectQuery(record.getString(8));
      res.setParallelLoadQuery(record.getString(9));
      res.setParallelLoadColumn(record.getString(10));
      res.setParallelLoadNum(record.getInt(11));
      res.setSelectColumn(record.getString(12));
      res.setPartitioned(record.getBoolean(13));
      res.setAutoloading(record.getBoolean(14));

      return res;
    }
    return null;
  }

  /**
   * Find a source in the Metastore by its id.
   *
   * @param id Id of the source.
   * @return Found source or null otherwise.
   * @throws SQLException         When something goes wrong with the SQL command.
   * @throws NullPointerException When the source's connection is missing.
   */
  private Source getById(Long id) throws SQLException, NullPointerException {
    PreparedStatement select = db.prepareStatement(SELECT_BY_ID);
    select.setLong(1, id);
    ResultSet record = select.executeQuery();
    if (record.next()) {
      return getSource(record);
    } else {
      return null;
    }
  }

  @Override
  public SourceResult save(Source source, Table table, boolean overwrite) {
    SourceResult result = new SourceResult();
    result.setNewResult(source);

    if (table != null && !table.isPartitioned() && source.isPartitioned()) {
      result.setSuccess(false);
      result.setMessage("An unpartitioned Table cannot have a partitioned Source. " +
              "(The other way around it is possible)");
      return result;
    }

    try {
      if (source.getConnection() == null) {
        throw new NullPointerException(
                "The connection in this source "
                        + "(id = "
                        + source.getId()
                        + ") is null. It needs to be set.");
      }
      Connection connection = null;
      if (source.getConnection().getName() != null) {
        connection = connectionDAO.getByName(source.getConnection().getName());
      } else if (source.getConnection().getId() != null) {
        connection = connectionDAO.getById(source.getConnection().getId());
      }
      if (connection == null) {
        throw new NullPointerException(
                "The connection for this source "
                        + "(id = "
                        + source.getId()
                        + ") is missing in the Metastore.");
      } else {
        source.setConnection(connection);
      }
      if (!overwrite) {
        Source oldSource = getById(source.getId());
        if (oldSource != null) {
          result.setOldResult(oldSource);

          if (!oldSource.equals(source)) {
            result.setMessage(
                    String.format(
                            "A Source with id=%d and different values already exists in the Metastore.",
                            source.getId()));
            result.setSuccess(false);
          } else {
            result.setSuccess(true);
          }

          return result;
        }
      } else {
        deleteById(source.getId());
      }
      saveToDb(source);
      result.setSuccess(true);
      result.setOldResult(source);
    } catch (SQLException | NullPointerException e) {
      result.setError(true);
      result.setException(e);
      result.setMessage(e.getMessage());
    }
    return result;
  }

  /**
   * Deletes a source by its id.
   *
   * @param id Source id
   */
  private void deleteById(Long id) throws SQLException {
    PreparedStatement delete = db.prepareStatement(DELETE_BY_ID);
    delete.setLong(1, id);
    delete.executeUpdate();
  }

  @Override
  public SourceResult[] save(Source[] sources, Table table, boolean overwrite) {
    SourceResult[] results = new SourceResult[sources.length];

    for (int i = 0; i < sources.length; i++) {
      results[i] = save(sources[i], table, overwrite);
    }

    return results;
  }

  @Override
  public Source[] getSourcesByTableId(Long tableId) throws SQLException, NullPointerException {
    PreparedStatement select;
    select = db.prepareStatement(SELECT_BY_TABLE_ID);
    select.setLong(1, tableId);

    ResultSet record = select.executeQuery();
    ArrayList<Source> sources = new ArrayList<>();

    while (record.next()) {
      Source res = getSource(record);

      sources.add(res);
    }

    return sources.toArray(new Source[0]);
  }

  @Override
  public void deleteSourcesByTableId(Long tableId) throws SQLException {
    PreparedStatement statement = db.prepareStatement(DELETE_BY_TABLE_ID);
    statement.setLong(1, tableId);
    statement.executeUpdate();
  }
}
