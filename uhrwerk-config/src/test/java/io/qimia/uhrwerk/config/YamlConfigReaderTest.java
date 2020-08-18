package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Test;

class YamlConfigReaderTest {
  @Test
  public void readConnectionsTest() {
    Connection[] connections =
        (new YamlConfigReader()).readConnections("config/connection-config.yml");
    for (Connection conn : connections) {
      System.out.println(conn);
    }
  }

  @Test
  public void readDagTest() {
    Dag dag =
            (new YamlConfigReader()).readDag("config/dag-config.yml");
      System.out.println(dag);
  }

  @Test
  public void readEnvTest() {
    Metastore metastore =
            (new YamlConfigReader()).readEnv("config/env-config.yml");
      System.out.println(metastore);
  }

  @Test
  public void readTablesTest() {
    Table table =
            (new YamlConfigReader()).readTable("config/table1-config.yml");
    System.out.println(table);
  }



  @Test
  public void yamlArray() {}
}
