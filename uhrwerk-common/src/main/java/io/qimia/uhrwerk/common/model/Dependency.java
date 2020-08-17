package io.qimia.uhrwerk.common.model;

import java.nio.file.Paths;
import java.util.Objects;

public class Dependency {
  Long id;
  Long tableId;
  Long targetId;
  String area;
  String vertical;
  String tableName;
  String format;
  String version;
  PartitionTransformType transformType;
  PartitionUnit transformPartitionUnit;
  int transformPartitionSize;

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

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * Concatenates area, vertical, table, version, and format into a path. Either with slashes for a
   * file system or with dashes and a dot for jdbc.
   *
   * @param fileSystem Whether the path is for a file system or for jdbc.
   * @return The concatenated path.
   */
  public String getPath(Boolean fileSystem) {
    if (fileSystem) {
      return Paths.get(
              "area=",
              area,
              "vertical=",
              vertical,
              "table=",
              tableName,
              "version=",
              version,
              "format=",
              format)
          .toString();
    } else { // jdbc
      return area + "-" + vertical + "." + tableName + "-" + version;
    }
  }

  public PartitionTransformType getTransformType() {
    return transformType;
  }

  public void setTransformType(PartitionTransformType transformType) {
    this.transformType = transformType;
  }

  public PartitionUnit getTransformPartitionUnit() {
    return transformPartitionUnit;
  }

  public void setTransformPartitionUnit(PartitionUnit transformPartitionUnit) {
    this.transformPartitionUnit = transformPartitionUnit;
  }

  public int getTransformPartitionSize() {
    return transformPartitionSize;
  }

  public void setTransformPartitionSize(int transformPartitionSize) {
    this.transformPartitionSize = transformPartitionSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Dependency that = (Dependency) o;
    return transformPartitionSize == that.transformPartitionSize
        && Objects.equals(area, that.area)
        && Objects.equals(vertical, that.vertical)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(format, that.format)
        && Objects.equals(version, that.version)
        && transformType == that.transformType
        && transformPartitionUnit == that.transformPartitionUnit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        area,
        vertical,
        tableName,
        format,
        version,
        transformType,
        transformPartitionUnit,
        transformPartitionSize);
  }

  @Override
  public String toString() {
    return "Dependency{"
        + "area='"
        + area
        + '\''
        + ", vertical='"
        + vertical
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", format='"
        + format
        + '\''
        + ", version='"
        + version
        + '\''
        + ", transformType="
        + transformType
        + ", transformPartitionUnit="
        + transformPartitionUnit
        + ", transformPartitionSize="
        + transformPartitionSize
        + '}';
  }
}