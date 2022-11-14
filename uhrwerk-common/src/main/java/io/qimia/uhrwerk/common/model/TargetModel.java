package io.qimia.uhrwerk.common.model;

import com.google.common.base.Objects;
import java.io.Serializable;

public class TargetModel extends StateModel implements Comparable<TargetModel>, Serializable {

  private static final long serialVersionUID = -3350529307251768851L;
  String format;
  Long tableId;
  Long connectionId;
  ConnectionModel connection;
  TableModel table;

  public TargetModel(TargetModelBuilder builder) {
    super(builder.deactivatedTs);
    this.id = builder.id;
    this.tableId = builder.tableId;
    this.connectionId = builder.connectionId;
    this.connection = builder.connection;
    this.table = builder.table;
    this.format = builder.format;
  }

  public static TargetModelBuilder builder() {
    return new TargetModelBuilder();
  }

  public Long getTableId() {
    return tableId;
  }

  public void setTableId(Long tableId) {
    this.tableId = tableId;
  }

  public Long getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(Long connectionId) {
    this.connectionId = connectionId;
  }

  public ConnectionModel getConnection() {
    return connection;
  }

  public void setConnection(ConnectionModel Connection) {
    this.connection = Connection;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  @Override
  public int compareTo(TargetModel o) {
    return this.getId().compareTo(o.getId());
  }

  public TableModel getTable() {
    return table;
  }

  public void setTable(TableModel table) {
    this.table = table;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TargetModel)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TargetModel that = (TargetModel) o;
    return Objects.equal(getTableId(), that.getTableId())
        && Objects.equal(getConnectionId(), that.getConnectionId())
        && Objects.equal(getConnection(), that.getConnection())
        && Objects.equal(getTable(), that.getTable());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(), getId(), getTableId(), getConnectionId(), getConnection(), getTable());
  }

  @Override
  public String toString() {
    return "TargetModel{" +
        "format='" + format + '\'' +
        ", tableId=" + tableId +
        ", connectionId=" + connectionId +
        ", connection=" + connection +
        '}';
  }
}
