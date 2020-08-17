package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Env;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

public class MetastoreTest {

  @Test
  public void test() {
    InputStream stream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("config/env-config.yml");
    Yaml yaml = new Yaml();
    Env env = yaml.loadAs(stream, Env.class);
    System.out.println(env);
  }
}
