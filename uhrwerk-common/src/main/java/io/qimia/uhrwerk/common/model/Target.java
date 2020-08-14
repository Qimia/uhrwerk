package io.qimia.uhrwerk.common.model;

import java.util.Objects;

public class Target {

  Connection connection;
  String format;

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
    return "Target{" +
            "connection=" + connection +
            ", format='" + format + '\'' +
            '}';
  }
}
