package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel;
import io.qimia.uhrwerk.common.metastore.model.TableModel;

public interface ConfigService {
  ConnectionModel save(ConnectionModel Connection);

  TableModel tableConfig(String area, String vertical, String name, String version);

  TableModel tableConfig(TableModel table);
}
