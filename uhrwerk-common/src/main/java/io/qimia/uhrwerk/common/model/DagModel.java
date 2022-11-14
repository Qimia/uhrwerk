package io.qimia.uhrwerk.common.model;

import java.io.Serializable;
import java.util.Arrays;

public class DagModel implements Serializable {

  private static final long serialVersionUID = -1481989945913775459L;

  ConnectionModel[] Connections;
  TableModel[] tables;

  public ConnectionModel[] getConnections() {
    return Connections;
  }

  public void setConnections(ConnectionModel[] Connections) {
    this.Connections = Connections;
  }

  public TableModel[] getTables() {
    return tables;
  }

  public void setTables(TableModel[] tables) {
    this.tables = tables;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DagModel dag = (DagModel) o;
    return Arrays.equals(Connections, dag.Connections) && Arrays.equals(tables, dag.tables);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(Connections);
    result = 31 * result + Arrays.hashCode(tables);
    return result;
  }

  @Override
  public String toString() {
    return "Dag{"
        + "connections="
        + Arrays.toString(Connections)
        + ", tables="
        + Arrays.toString(tables)
        + '}';
  }
}
