package io.qimia.uhrwerk.config.model;

import java.util.Objects;

public class Dependency {
  String area;
  String vertical;
  String tableName;
  String format;
  String version;

  public Dependency() {}

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getArea() {
    return area;
  }

  public void setArea(String area) {
    this.area = area;
  }

  public String getVertical() {
    return vertical;
  }

  public void setVertical(String vertical) {
    this.vertical = vertical;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Dependency that = (Dependency) o;
    return Objects.equals(area, that.area)
        && Objects.equals(vertical, that.vertical)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(format, that.format)
        && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(area, vertical, tableName, format, version);
  }

  @Override
  public String toString() {
    return "Dependency{" +
            "area='" + area + '\'' +
            ", vertical='" + vertical + '\'' +
            ", tableName='" + tableName + '\'' +
            ", format='" + format + '\'' +
            ", version='" + version + '\'' +
            '}';
  }
}
