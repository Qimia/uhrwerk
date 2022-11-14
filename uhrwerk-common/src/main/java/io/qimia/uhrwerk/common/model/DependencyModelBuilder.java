package io.qimia.uhrwerk.common.model;

import org.jetbrains.annotations.NotNull;

public class DependencyModelBuilder extends StateModelBuilder<DependencyModelBuilder> {

  Long id;
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

  public DependencyModelBuilder id(Long id) {
    this.id = id;
    return this;
  }

  public DependencyModelBuilder tableId(@NotNull Long tableId) {
    this.tableId = tableId;
    return this;
  }

  public DependencyModelBuilder dependencyTargetId(@NotNull Long dependencyTargetId) {
    this.dependencyTargetId = dependencyTargetId;
    return this;
  }

  public DependencyModelBuilder dependencyTableId(@NotNull Long dependencyTableId) {
    this.dependencyTableId = dependencyTableId;
    return this;
  }

  public DependencyModelBuilder table(TableModel table) {
    this.table = table;
    return this;
  }

  public DependencyModelBuilder dependencyTable(TableModel dependencyTable) {
    this.dependencyTable = dependencyTable;
    return this;
  }

  public DependencyModelBuilder dependencyTarget(TargetModel dependencyTarget) {
    this.dependencyTarget = dependencyTarget;
    return this;
  }

  public DependencyModelBuilder area(String area) {
    this.area = area;
    return this;
  }

  public DependencyModelBuilder vertical(String vertical) {
    this.vertical = vertical;
    return this;
  }

  public DependencyModelBuilder tableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public DependencyModelBuilder format(String format) {
    this.format = format;
    return this;
  }

  public DependencyModelBuilder version(String version) {
    this.version = version;
    return this;
  }

  public DependencyModelBuilder transformType(PartitionTransformType transformType) {
    this.transformType = transformType;
    return this;
  }

  public DependencyModelBuilder transformPartitionUnit(PartitionUnit transformPartitionUnit) {
    this.transformPartitionUnit = transformPartitionUnit;
    return this;
  }

  public DependencyModelBuilder transformPartitionSize(int transformPartitionSize) {
    this.transformPartitionSize = transformPartitionSize;
    return this;
  }

  public DependencyModel build() {
    return new DependencyModel(this);
  }

  @Override
  DependencyModelBuilder getThis() {
    return this;
  }
}
