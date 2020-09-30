package io.qimia.uhrwerk.common.model;

import net.openhft.hashing.LongHashFunction;

import java.io.Serializable;
import java.util.Objects;

public class Dependency implements Comparable<Dependency>, Serializable {

  private static final long serialVersionUID = -1631196520233395698L;

  Long id;
  Long tableId;
  Long dependencyTargetId;
  Long dependencyTableId;
  String area;
  String vertical;
  String tableName;
  String format;
  String version;
  PartitionTransformType transformType;
  PartitionUnit transformPartitionUnit;
  int transformPartitionSize;

  public void setKey() {
    StringBuilder res =
        new StringBuilder()
            .append(this.getArea())
            .append(this.getVertical())
            .append(this.getTableName())
            .append(this.getVersion());
    long depTableId = LongHashFunction.xx().hashChars(res);
    setDependencyTableId(depTableId);
    StringBuilder res2 = new StringBuilder().append(depTableId).append(this.getFormat());
    long targetId = LongHashFunction.xx().hashChars(res2);
    setDependencyTargetId(targetId);
    StringBuilder res3 =
        new StringBuilder()
            .append(this.getTableId())
            .append(depTableId)
            .append(this.dependencyTargetId);
    long id = LongHashFunction.xx().hashChars(res3);
    this.setId(id);
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getTableId() {
    return tableId;
  }

  public void setTableId(Long tableId) {
    this.tableId = tableId;
  }

  public Long getDependencyTargetId() {
    return dependencyTargetId;
  }

  public void setDependencyTargetId(Long dependencyTargetId) {
    this.dependencyTargetId = dependencyTargetId;
  }

  public Long getDependencyTableId() {
    return dependencyTableId;
  }

  public void setDependencyTableId(Long dependencyTableId) {
    this.dependencyTableId = dependencyTableId;
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
        + "id="
        + id
        + ", tableId="
        + tableId
        + ", dependencyTargetId="
        + dependencyTargetId
        + ", dependencyTableId="
        + dependencyTableId
        + ", area='"
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

  @Override
  public int compareTo(Dependency o) {
    return this.getId().compareTo(o.getId());
  }
}
