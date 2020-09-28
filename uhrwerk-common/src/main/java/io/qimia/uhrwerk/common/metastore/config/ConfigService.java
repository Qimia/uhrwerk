package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.Table;

public interface ConfigService {
  Connection save(Connection connection);

  Table tableConfig(String area, String vertical, String name, String version);

  Table tableConfig(Table table);
}
