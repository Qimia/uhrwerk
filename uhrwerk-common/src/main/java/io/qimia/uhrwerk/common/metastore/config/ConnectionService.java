package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.ConnectionModel;
import java.sql.SQLException;
import java.util.List;

public interface ConnectionService {
  ConnectionResult save(ConnectionModel Connection, boolean overwrite);
  ConnectionModel getById(Long id) throws SQLException;
  ConnectionModel getByHashKey(Long id) throws SQLException;

  List<ConnectionModel> getAllTableDeps(Long tableId) throws SQLException;
}
