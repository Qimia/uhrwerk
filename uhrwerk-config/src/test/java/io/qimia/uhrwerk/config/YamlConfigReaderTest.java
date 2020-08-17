package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.Connection;
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
  public void yamlArray() {}
}
