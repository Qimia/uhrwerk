package io.qimia.uhrwerk.common.cli;

import java.util.Objects;

public class Config {
  String envConfig = "env-config.yml";
  String connConfig = "connection-config.yml";
  String tableConfig = "table-config.yml";
  String dagConfig = "dag-config.yml";

  public String getEnvConfig() {
    return envConfig;
  }

  public void setEnvConfig(String envConfig) {
    this.envConfig = envConfig;
  }

  public String getConnConfig() {
    return connConfig;
  }

  public void setConnConfig(String connConfig) {
    this.connConfig = connConfig;
  }

  public String getTableConfig() {
    return tableConfig;
  }

  public void setTableConfig(String tableConfig) {
    this.tableConfig = tableConfig;
  }

  public String getDagConfig() {
    return dagConfig;
  }

  public void setDagConfig(String dagConfig) {
    this.dagConfig = dagConfig;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Config config = (Config) o;
    return Objects.equals(envConfig, config.envConfig)
        && Objects.equals(connConfig, config.connConfig)
        && Objects.equals(tableConfig, config.tableConfig)
        && Objects.equals(dagConfig, config.dagConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(envConfig, connConfig, tableConfig, dagConfig);
  }

  @Override
  public String toString() {
    return "Config{" +
            "envConfig='" + envConfig + '\'' +
            ", connConfig='" + connConfig + '\'' +
            ", tableConfig='" + tableConfig + '\'' +
            ", dagConfig='" + dagConfig + '\'' +
            '}';
  }
}
