package dao;

import io.qimia.uhrwerk.common.metastore.config.ConnectionResult;
import io.qimia.uhrwerk.common.metastore.config.ConnectionService;
import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.ConnectionType;
import io.qimia.uhrwerk.common.model.Dependency;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnectionDAO implements ConnectionService {

  private static final String INSERT_JDBC_CONN =
      "INSERT INTO CONNECTION(name, type, path, version,jdbc_url, jdbc_driver, jdbc_user, jdbc_pass)\n"
          + "VALUES(?,?,?,?,?,?,?,?)";

  public static Long saveJdbc(java.sql.Connection db, Connection connection) throws SQLException {
    PreparedStatement insert =
        db.prepareStatement(INSERT_JDBC_CONN, Statement.RETURN_GENERATED_KEYS);
    setCommonValues(insert, connection);
    insert.setString(5, connection.getJdbcUrl());
    insert.setString(6, connection.getJdbcDriver());
    insert.setString(7, connection.getJdbcUser());
    insert.setString(8, connection.getJdbcPass());
    return JdbcBackendUtils.singleRowUpdate(insert);
  }

  private static final String INSERT_AWS_CONN =
      "INSERT INTO CONNECTION(name, type, path,version, aws_access_key_id, aws_secret_access_key)\n"
          + "VALUES(?,?,?,?,?,?)";

  public static Long saveAWS(java.sql.Connection db, Connection connection) throws SQLException {
    PreparedStatement insert =
        db.prepareStatement(INSERT_AWS_CONN, Statement.RETURN_GENERATED_KEYS);
    setCommonValues(insert, connection);
    insert.setString(5, connection.getAwsAccessKeyID());
    insert.setString(6, connection.getAwsSecretAccessKey());
    return JdbcBackendUtils.singleRowUpdate(insert);
  }

  private static final String INSERT_FS_CONN =
      "INSERT INTO CONNECTION(name, type, path,version)\n" + "VALUES(?,?,?,?)";

  public static Long saveFS(java.sql.Connection db, Connection connection) throws SQLException {
    PreparedStatement insert = db.prepareStatement(INSERT_FS_CONN, Statement.RETURN_GENERATED_KEYS);
    setCommonValues(insert, connection);
    return JdbcBackendUtils.singleRowUpdate(insert);
  }

  private static void setCommonValues(PreparedStatement insert, Connection connection)
      throws SQLException {
    insert.setString(1, connection.getName());
    insert.setString(2, connection.getType().name());
    insert.setString(3, connection.getPath());
    insert.setString(4, connection.getVersion());
  }

  private static final String SELECT_BY_NAME_VERSION =
      "SELECT name,\n"
          + "       type,\n"
          + "       path,\n"
          + "       version,\n"
          + "       jdbc_url,\n"
          + "       jdbc_driver,\n"
          + "       jdbc_user,\n"
          + "       jdbc_pass,\n"
          + "       aws_access_key_id,\n"
          + "       aws_secret_access_key\n"
          + "FROM CONNECTION\n"
          + "WHERE name =?\n"
          + "  AND version =?";

  public static Connection getByNameVersion(java.sql.Connection db, String name, String version)
      throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_BY_NAME_VERSION);
    select.setString(1, name);
    select.setString(2, version);

    return getConnection(select);
  }

  private static final String SELECT_BY_NAME =
      "SELECT name,\n"
          + "       type,\n"
          + "       path,\n"
          + "       version,\n"
          + "       jdbc_url,\n"
          + "       jdbc_driver,\n"
          + "       jdbc_user,\n"
          + "       jdbc_pass,\n"
          + "       aws_access_key_id,\n"
          + "       aws_secret_access_key\n"
          + "FROM CONNECTION\n"
          + "WHERE name =?\n";

  public static Connection getByName(java.sql.Connection db, String name) throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_BY_NAME);
    select.setString(1, name);
    return getConnection(select);
  }

  private static final String SELECT_DEPENDENCY_CONN =
      "SELECT cn.name,\n"
          + "       cn.type,\n"
          + "       cn.path,\n"
          + "       cn.version,\n"
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

  public static Connection getDependencyConnection(java.sql.Connection db, Dependency dependency)
      throws SQLException {
    PreparedStatement select = db.prepareStatement(SELECT_DEPENDENCY_CONN);
    select.setString(1, dependency.getArea());
    select.setString(2, dependency.getVertical());
    select.setString(3, dependency.getTableName());
    select.setString(3, dependency.getVersion());
    select.setString(3, dependency.getFormat());
    return getConnection(select);
  }

  private static Connection getConnection(PreparedStatement select) throws SQLException {
    ResultSet record = select.executeQuery();
    if (record.next()) {
      Connection res = new Connection();
      res.setName(record.getString(1));
      res.setType(ConnectionType.valueOf(record.getString(2)));
      res.setPath(record.getString(3));
      res.setVersion(record.getString(4));
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

  @Override
  public ConnectionResult save(java.sql.Connection db, Connection connection, boolean overwrite) {
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
      Long id = null;
      switch (connection.getType()) {
        case FS:
          id = saveFS(db, connection);
          break;
        case JDBC:
          id = saveJdbc(db, connection);
          break;
        case S3:
          id = saveAWS(db, connection);
          break;
        case GC:
        case ABS:
          break;
      }
      connection.setId(id);
      result.setSuccess(true);
      result.setOldConnection(connection);
    } catch (SQLException e) {
      result.setError(true);
      result.setException(e);
      result.setMessage(e.getMessage());
    }
    return result;
  }
}
