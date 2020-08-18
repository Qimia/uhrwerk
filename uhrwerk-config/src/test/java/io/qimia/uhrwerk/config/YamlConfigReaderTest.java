package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class YamlConfigReaderTest {
  @Test
  public void readConnectionsTest() {
    Connection[] connections =
        (new YamlConfigReader()).readConnections("config/connection-config.yml");
    for (Connection conn : connections) {
      System.out.println(conn);
    }
    assertEquals(connections[0].getName(),"mysql1");
    assertEquals(connections[1].getName(),"s3_test");
    assertEquals(connections[2].getName(),"local_filesystem_test");
    assertEquals(connections[0].getJdbcUrl(),"jdbc:mysql://localhost:3306");
    assertEquals(connections[1].getJdbcUrl(),null);
    assertEquals(connections[2].getJdbcUrl(),null);
    assertEquals(connections[0].getJdbcDriver(),"com.mysql.jdbc.Driver");
    assertEquals(connections[1].getJdbcDriver(),null);
    assertEquals(connections[2].getJdbcDriver(),null);
    assertEquals(connections[0].getJdbcUser(),"root");
    assertEquals(connections[1].getJdbcUser(),null);
    assertEquals(connections[2].getJdbcUser(),null);
    assertEquals(connections[0].getJdbcPass(),"mysql");
    assertEquals(connections[1].getJdbcPass(),null);
    assertEquals(connections[2].getJdbcPass(),null);
    assertEquals(connections[0].getPath(),null);
    assertEquals(connections[1].getPath(),"s3://bucketname/somesuffix/");
    assertEquals(connections[2].getPath(),"/path/to/local/datalake");
    assertEquals(connections[0].getAwsAccessKeyID(),null);
    assertEquals(connections[1].getAwsAccessKeyID(),"blabla");
    assertEquals(connections[2].getAwsAccessKeyID(),null);
    assertEquals(null  , connections[0].getAwsSecretAccessKey());
    assertEquals("yaya", connections[1].getAwsSecretAccessKey());
    assertEquals(null  , connections[2].getAwsSecretAccessKey());


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
    assertEquals(metastore.getJdbc_url(),"jdbc:mysql://localhost:53306/UHRWERK_METASTORE");
    assertEquals(metastore.getJdbc_driver(), "com.mysql.jdbc.Driver");
    assertEquals(metastore.getUser(), "UHRWERK_USER");
    assertEquals(metastore.getPass(), "Xq92vFqEKF7TB8H9");

  }

  @Test
  public void readTablesTest() {
    Table table =
            (new YamlConfigReader()).readTable("config/table1-config.yml");
    System.out.println(table);
    assertEquals(table.getId()  , table.getSources()[0].getTableId());
    assertEquals(table.getId()  , table.getTargets()[0].getTableId());
    assertEquals(table.getId()  , table.getDependencies()[0].getTableId());
    assertNotEquals(table.getDependencies()[0].getTargetId(),table.getTargets()[0].getId());

  }


  @Test
  public void yamlArray() {}
}
