package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Env;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetastoreTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  public void test() {
    InputStream stream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("config/env-config.yml");
    Yaml yaml = new Yaml();
    Env env = yaml.loadAs(stream, Env.class);
    logger.info(env);

    assertEquals("Xq92vFqEKF7TB8H9", env.getMetastore().getPass());
  }
}
