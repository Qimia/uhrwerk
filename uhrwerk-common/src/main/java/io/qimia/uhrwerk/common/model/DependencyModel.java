package io.qimia.uhrwerk.common.model;

import com.google.common.base.Objects;
import java.io.Serializable;

public class DependencyModel extends StateModel
    implements Comparable<DependencyModel>, Serializable {

  private static final long serialVersionUID = -1631196520233395698L;

  Long tableId;
  Long dependencyTargetId;
  Long dependencyTableId;
  TableModel table;
  TableModel dependencyTable;
  TargetModel dependencyTarget;
  String area;
  String vertical;
  String tableName;
  String format;
  String version;
  PartitionTransformType transformType;
  PartitionUnit transformPartitionUnit;
  int transformPartitionSize;

  public DependencyModel(DependencyModelBuilder builder) {
    super(builder.deactivatedTs);
    this.id = builder.id;
    this.tableId = builder.tableId;
    this.dependencyTargetId = builder.dependencyTargetId;
    this.dependencyTableId = builder.dependencyTableId;
    this.table = builder.table;
    this.dependencyTable = builder.dependencyTable;
    this.dependencyTarget = builder.dependencyTarget;
    this.area = builder.area;
    this.vertical = builder.vertical;
    this.tableName = builder.tableName;
    this.format = builder.format;
    this.version = builder.version;
    this.transformType = builder.transformType;
    this.transformPartitionUnit = builder.transformPartitionUnit;
    this.transformPartitionSize = builder.transformPartitionSize;
  }

  public static DependencyModelBuilder builder() {
    return new DependencyModelBuilder();
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
  public int compareTo(DependencyModel o) {
    return this.getId().compareTo(o.getId());
  }

  public TableModel getTable() {
    return table;
  }

  public void setTable(TableModel table) {
    this.table = table;
  }

  public TableModel getDependencyTable() {
    return dependencyTable;
  }

  public void setDependencyTable(TableModel dependencyTable) {
    this.dependencyTable = dependencyTable;
  }

  public TargetModel getDependencyTarget() {
    return dependencyTarget;
  }

  public void setDependencyTarget(TargetModel dependencyTarget) {
    this.dependencyTarget = dependencyTarget;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DependencyModel)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DependencyModel that = (DependencyModel) o;
    return getTransformPartitionSize() == that.getTransformPartitionSize()
        && Objects.equal(getTableId(), that.getTableId())
        && Objects.equal(getDependencyTargetId(), that.getDependencyTargetId())
        && Objects.equal(getDependencyTableId(), that.getDependencyTableId())
        && Objects.equal(getTable(), that.getTable())
        && Objects.equal(getDependencyTable(), that.getDependencyTable())
        && Objects.equal(getDependencyTarget(), that.getDependencyTarget())
        && Objects.equal(getArea(), that.getArea())
        && Objects.equal(getVertical(), that.getVertical())
        && Objects.equal(getTableName(), that.getTableName())
        && Objects.equal(getFormat(), that.getFormat())
        && Objects.equal(getVersion(), that.getVersion())
        && getTransformType() == that.getTransformType()
        && getTransformPartitionUnit() == that.getTransformPartitionUnit();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(),
        getDependencyTargetId(),
        getDependencyTableId(),
        getTable(),
        getDependencyTable(),
        getDependencyTarget(),
        getArea(),
        getVertical(),
        getTableName(),
        getFormat(),
        getVersion(),
        getTransformType(),
        getTransformPartitionUnit(),
        getTransformPartitionSize());
  }

  @Override
  public String toString() {
    return "DependencyModel{"
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
}
