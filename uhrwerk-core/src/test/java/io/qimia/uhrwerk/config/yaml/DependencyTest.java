package io.qimia.uhrwerk.config.yaml;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.Map;

class DependencyTest {
  @Test
  void parseDependency() {
    String yamlString =
        "connectionName: connection_name\n"
            + "tableName: tableOne  # On DB\n"
            + "format: \"jdbc\"\n"
            + "area: staging\n"
            + "vertical: sourcedb_1\n"
            + "version: 1\n"
            + "type: \"oneOnone\"\n"
            + "partitionSize: \"6h\"\n"
            + "partitionCount: 1";
    Yaml yaml = new Yaml();

    Map<String, Object> obj = yaml.load(yamlString);
    System.out.println(obj);
    Dependency dependency = yaml.loadAs(yamlString, Dependency.class);
    System.out.println(dependency);
  }
}
