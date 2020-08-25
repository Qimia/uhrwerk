package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.ConnectionResult;
import io.qimia.uhrwerk.common.metastore.config.ConnectionService;
import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.ConnectionType;
import io.qimia.uhrwerk.common.model.Dependency;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ConnectionDAO implements ConnectionService {

  private java.sql.Connection db;

  public ConnectionDAO() {}

  public ConnectionDAO(java.sql.Connection db) {
    this.db = db;
  }

  public java.sql.Connection getDb() {
    return db;
  }

  public void setDb(java.sql.Connection db) {
    this.db = db;
  }

  private static final String UPSERT_CONN =
      "INSERT INTO CONNECTION(id, name, type, path, jdbc_url, jdbc_driver, jdbc_user, jdbc_pass, aws_access_key_id, aws_secret_access_key)\n"
          + "VALUES(?,?,?,?,?,?,?,?,?,?) "
          + "ON DUPLICATE KEY UPDATE \n"
          + "type=?, path=?, jdbc_url=?, jdbc_driver=?, jdbc_user=?, jdbc_pass=?, aws_access_key_id=?, aws_secret_access_key=?";

  public Long save(Connection connection) throws SQLException {
    PreparedStatement insert = db.prepareStatement(UPSERT_CONN, Statement.RETURN_GENERATED_KEYS);
    // common columns values
    insert.setLong(1, connection.getId());
    insert.setString(2, connection.getName());
    insert.setString(3, connection.getType().name());
    insert.setString(4, connection.getPath());
    // jdbc columns values
    insert.setString(5, connection.getJdbcUrl());
    insert.setString(6, connection.getJdbcDriver());
    insert.setString(7, connection.getJdbcUser());
    insert.setString(8, connection.getJdbcPass());
    // aws columns values
    insert.setString(9, connection.getAwsAccessKeyID());
    insert.setString(10, connection.getAwsSecretAccessKey());
    // common columns assignment_list
    insert.setString(11, connection.getType().name());
    insert.setString(12, connection.getPath());
    // jdbc columns assignment_list
    insert.setString(13, connection.getJdbcUrl());
    insert.setString(14, connection.getJdbcDriver());
    insert.setString(15, connection.getJdbcUser());
    insert.setString(16, connection.getJdbcPass());
    // aws columns assignment_list
    insert.setString(17, connection.getAwsAccessKeyID());
    insert.setString(18, connection.getAwsSecretAccessKey());
    return JdbcBackendUtils.singleRowUpdate(insert);
  }

  private static final String SELECT_BY_NAME =
      "SELECT id, name,\n"
          + "       type,\n"
          + "       path,\n"
          + "       jdbc_url,\n"
          + "       jdbc_driver,\n"
          + "       jdbc_user,\n"
          + "       jdbc_pass,\n"
          + "       aws_access_key_id,\n"
          + "       aws_secret_access_key\n"
          + "FROM CONNECTION\n"
          + "WHERE name =?\n";

  public Connection getByName(java.sql.Connection db, String name) throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_BY_NAME);
    select.setString(1, name);
    return getConnection(select);
  }

  private static final String SELECT_BY_ID =
      "SELECT id, name,\n"
          + "       type,\n"
          + "       path,\n"
          + "       jdbc_url,\n"
          + "       jdbc_driver,\n"
          + "       jdbc_user,\n"
          + "       jdbc_pass,\n"
          + "       aws_access_key_id,\n"
          + "       aws_secret_access_key\n"
          + "FROM CONNECTION\n"
          + "WHERE id =?\n";

  public Connection getById(Long id) throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_BY_ID);
    select.setLong(1, id);
    Connection connection = getConnection(select);
    if (connection != null) {
      connection.setId(id);
    }

    return connection;
  }

  private static final String SELECT_DEPENDENCY_CONN =
      "SELECT cn.id, cn.name,\n"
          + "       cn.type,\n"
          + "       cn.path,\n"
          + "       cn.jdbc_url,\n"
          + "       cn.jdbc_driver,\n"
          + "       cn.jdbc_user,\n"
          + "       cn.jdbc_pass,\n"
          + "       cn.aws_access_key_id,\n"
          + "       cn.aws_secret_access_key\n"
          + "FROM TABLE_ AS tl\n"
          + "         JOIN TARGET tr ON tl.id = tr.table_id\n"
          + "         JOIN CONNECTION cn ON tr.connection_id = cn.id\n"
          + "WHERE tl.area = ?\n"
          + "  AND tl.vertical = ?\n"
          + "  AND tl.name = ?\n"
          + "  AND tl.version = ?\n"
          + "  AND tr.format = ?";

  public Connection getDependencyConnection(Dependency dependency) throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_DEPENDENCY_CONN);
    select.setString(1, dependency.getArea());
    select.setString(2, dependency.getVertical());
    select.setString(3, dependency.getTableName());
    select.setString(3, dependency.getVersion());
    select.setString(3, dependency.getFormat());
    return getConnection(select);
  }

  private static final String SELECT_TABLE_DEPS_CONNS =
      "SELECT C.id,\n"
          + "       C.name,\n"
          + "       C.type,\n"
          + "       C.path,\n"
          + "       C.jdbc_url,\n"
          + "       C.jdbc_driver,\n"
          + "       C.jdbc_user,\n"
          + "       C.jdbc_pass,\n"
          + "       C.aws_access_key_id,\n"
          + "       C.aws_secret_access_key\n"
          + "FROM CONNECTION C\n"
          + "         JOIN TARGET T on C.id = T.connection_id\n"
          + "         JOIN DEPENDENCY D on T.id = D.dependency_target_id\n"
          + "WHERE D.table_id = ?";

  public Connection[] getTableDependenciesConnections(Long tableId) throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_TABLE_DEPS_CONNS);
    select.setLong(1, tableId);
    return getConnections(select);
  }

  public Connection getConnection(PreparedStatement select) throws SQLException {
    ResultSet record = select.executeQuery();
    if (record.next()) {
      Connection res = new Connection();
      res.setId(record.getLong(1));
      res.setName(record.getString(2));
      res.setType(ConnectionType.valueOf(record.getString(3)));
      res.setPath(record.getString(4));
      res.setJdbcUrl(record.getString(5));
      res.setJdbcDriver(record.getString(6));
      res.setJdbcUser(record.getString(7));
      res.setJdbcPass(record.getString(8));
      res.setAwsAccessKeyID(record.getString(9));
      res.setAwsSecretAccessKey(record.getString(10));
      return res;
    }
    return null;
  }

  public Connection[] getConnections(PreparedStatement select) throws SQLException {
    ResultSet record = select.executeQuery();
    List<Connection> list = new ArrayList<>();
    while (record.next()) {
      Connection connection = new Connection();
      connection.setId(record.getLong(1));
      connection.setName(record.getString(2));
      connection.setType(ConnectionType.valueOf(record.getString(3)));
      connection.setPath(record.getString(4));
      connection.setJdbcUrl(record.getString(5));
      connection.setJdbcDriver(record.getString(6));
      connection.setJdbcUser(record.getString(7));
      connection.setJdbcPass(record.getString(8));
      connection.setAwsAccessKeyID(record.getString(9));
      connection.setAwsSecretAccessKey(record.getString(10));
      list.add(connection);
    }
    return list.toArray(new Connection[list.size()]);
  }

  @Override
  public ConnectionResult save(Connection connection, boolean overwrite) {
    ConnectionResult result = new ConnectionResult();
    result.setNewConnection(connection);
    try {
      if (!overwrite) {
        Connection oldConnection = getByName(db, connection.getName());
        if (oldConnection != null) {
          result.setOldConnection(oldConnection);
          result.setMessage(
              String.format(
                  "A Connection with name=%s already exists in the Metastore.",
                  connection.getName()));
          return result;
        }
      }
      save(connection);
      result.setSuccess(true);
    } catch (SQLException e) {
      result.setError(true);
      result.setException(e);
      result.setMessage(e.getMessage());
    }
    return result;
  }

  public ConnectionResult[] save(Connection[] connections, boolean overwrite) {
    ConnectionResult[] resultSet = new ConnectionResult[connections.length];
    for (int i = 0; i < connections.length; i++) {
      ConnectionResult connResult = save(connections[i], overwrite);
      resultSet[i] = connResult;
    }
    return resultSet;
  }
}
