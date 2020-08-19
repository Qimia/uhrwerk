package io.qimia.uhrwerk.common.model;

import net.openhft.hashing.LongHashFunction;

import java.util.Objects;

public class Target {

  Long tableId;
  Long id;
  Connection connection;
  String format;

  public void setKey() {
    StringBuilder res = new StringBuilder().append(tableId).append(format);
    long id = LongHashFunction.xx().hashChars(res);
    setId(id);
  }

  public Long getTableId() {
    return tableId;
  }

  public void setTableId(Long tableId) {
    this.tableId = tableId;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Target target = (Target) o;
    return Objects.equals(connection, target.connection) && Objects.equals(format, target.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connection, format);
  }

  @Override
  public String toString() {
    return "Target{"
            + "tableId='"
            + tableId
            + '\''
            + ", id='"
            + id
            + '\''
            + ", connection="
            + connection
            + ", format='"
            + format
            + '\''
            + '}';
  }
}
