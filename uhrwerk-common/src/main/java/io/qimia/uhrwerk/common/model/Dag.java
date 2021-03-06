package io.qimia.uhrwerk.common.model;

import java.io.Serializable;
import java.util.Arrays;

public class Dag implements Serializable {

  private static final long serialVersionUID = -1481989945913775459L;

  Connection[] connections;
  Table[] tables;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Dag dag = (Dag) o;
    return Arrays.equals(connections, dag.connections) && Arrays.equals(tables, dag.tables);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(connections);
    result = 31 * result + Arrays.hashCode(tables);
    return result;
  }

  @Override
  public String toString() {
    return "Dag{"
        + "connections="
        + Arrays.toString(connections)
        + ", tables="
        + Arrays.toString(tables)
        + '}';
  }
}
