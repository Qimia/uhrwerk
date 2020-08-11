package io.qimia.uhrwerk.config.yaml;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DependencyTest {
  @Test
  void parseDependency() {
    String yamlString =
        "connectionName: connection_name\n"
            + "tableName: staging_source_table  # On DB\n"
            + "format: \"jdbc\"\n"
            + "version: 1\n"
            + "partitionSize: \"6h\"\n"
            + "partitionQuery: \"SELECT id FROM <path> WHERE created_at >= '<lower_bound>' and created_at \\\\< '<upper_bound>'\"\n"
            + "partitionColumn: \"id\"\n"
            + "selectQuery: \"table_test_2_select_query.sql\"\n"
            + "queryColumn: \"created_at\"\n"
            + "sparkReaderNumPartitions: 40";
    Yaml yaml = new Yaml();

    Map<String, Object> obj = yaml.load(yamlString);
    System.out.println(obj);
  }
}
