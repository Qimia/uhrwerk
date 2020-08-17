package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.Dag;
import io.qimia.uhrwerk.config.representation.Metastore;
import io.qimia.uhrwerk.config.representation.Table;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

public class YamlConfigReader {
  public io.qimia.uhrwerk.common.model.Connection[] readConnections(String file) {
    Yaml yaml = new Yaml();
    InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    Connection[] connections = yaml.loadAs(stream, Connection[].class);
    io.qimia.uhrwerk.common.model.Connection[] result =
        new io.qimia.uhrwerk.common.model.Connection[connections.length];
    for (int i = 0; i < connections.length; i++) {
      io.qimia.uhrwerk.common.model.Connection conn =
          new io.qimia.uhrwerk.common.model.Connection();
      result[i] = conn;
      conn.setName(connections[i].getName());
    }

    return result;
  }

  public io.qimia.uhrwerk.common.model.Dag readDag(String file) {
    return null;
  }

  public io.qimia.uhrwerk.common.model.Metastore readEnv(String file) {
    return null;
  }

  public Table readTable(String file) {
    return null;
  }
}
