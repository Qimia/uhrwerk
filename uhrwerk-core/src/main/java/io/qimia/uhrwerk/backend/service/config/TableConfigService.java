package io.qimia.uhrwerk.backend.service.config;

import io.qimia.uhrwerk.config.model.Table;

public interface TableConfigService {
  public Table tableConfig(String area, String vertical, String name, String version);

  public Table tableConfig(Table table);
}
