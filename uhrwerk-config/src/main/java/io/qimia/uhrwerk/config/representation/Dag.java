package io.qimia.uhrwerk.config.representation;

import java.util.Arrays;
import io.qimia.uhrwerk.config.ConfigException;

public class Dag {
  private Connection[] connections;
  private Table[] tables;

  public Connection[] getConnections() {
    return connections;
  }

  public void setConnections(Connection[] connections) {
    this.connections = connections;
  }

  public Table[] getTables() {
    return tables;
  }

  public void setTables(Table[] tables) {
    this.tables = tables;
  }

  public void validate(String path) {
    path += "/";
    if(connections==null){
      throw new ConfigException("Missing field:" + path + "connections");
    }
    else{
      for (Connection c: connections){
        c.validate(path);
      }
    }
    if(tables==null){
      throw new ConfigException("Missing field:" + path + "tables");
    }
    else{
      for (Table t: tables){
        t.validate(path);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Dag dag = (Dag) o;
    return Arrays.equals(connections, dag.connections) &&
            Arrays.equals(tables, dag.tables);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(connections);
    result = 31 * result + Arrays.hashCode(tables);
    return result;
  }

  @Override
  public String toString() {
    return "Dag{" +
            "connections=" + Arrays.toString(connections) +
            ", tables=" + Arrays.toString(tables) +
            '}';
  }
}
