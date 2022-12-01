package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.config.representation.Env;
import io.qimia.uhrwerk.config.representation.YamlUtils;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetastoreTest {
  private final Logger logger = LoggerFactory.getLogger(MetastoreTest.class);

  @Test
  public void test() throws IOException {
    InputStream stream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(
            "config/env-config-new.yml");
    Yaml yaml = new Yaml();
    Env env = YamlUtils.objectMapper().readValue(stream, Env.class);
    logger.info(env.toString());

    assertEquals("uhrwerk_db_passwd", env.getMetastore().getPassword());
  }
}
