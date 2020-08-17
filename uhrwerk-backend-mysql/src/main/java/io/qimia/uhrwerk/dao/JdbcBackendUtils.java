package io.qimia.uhrwerk.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcBackendUtils {
  public static Long singleRowUpdate(PreparedStatement statement) throws SQLException {
    statement.executeUpdate();
    ResultSet generatedKeys = statement.getGeneratedKeys();
    if (generatedKeys.next()) return generatedKeys.getLong(1);
    return null;
  }
}
