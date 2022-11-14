package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Env;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetastoreTest {
  private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

  @Test
  public void test() {
    InputStream stream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("config/env-config.yml");
    Yaml yaml = new Yaml();
    Env env = yaml.loadAs(stream, Env.class);
    logger.info(env.toString());

    assertEquals("Xq92vFqEKF7TB8H9", env.getMetastore().getPass());
  }
}
