package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Connection;

public interface ConnectionService {
  public ConnectionResult save(java.sql.Connection db, Connection connection, boolean overwrite);
}
