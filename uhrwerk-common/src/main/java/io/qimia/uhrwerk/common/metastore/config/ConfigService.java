package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.Table;

public interface ConfigService {
  public Connection save(Connection connection);
  public Table tableConfig(String area, String vertical, String name, String version);

  public Table tableConfig(Table table);
}
