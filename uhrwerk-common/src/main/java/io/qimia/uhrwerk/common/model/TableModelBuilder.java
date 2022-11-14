package io.qimia.uhrwerk.common.model;

import org.jetbrains.annotations.NotNull;

public class TableModelBuilder extends StateModelBuilder<TableModelBuilder> {

  Long id;
  String area;
  String vertical;
  String name;
  String version;
  String className;
  int parallelism;
  int maxBulkSize;
  PartitionUnit partitionUnit;
  int partitionSize;
  boolean partitioned;

  DependencyModel[] dependencies;
  SourceModel[] sources;
  TargetModel[] targets;

  public TableModelBuilder id(Long id) {
    this.id = id;
    return this;
  }

  public TableModelBuilder area(@NotNull String area) {
    this.area = area;
    return this;
  }

  public TableModelBuilder vertical(@NotNull String vertical) {
    this.vertical = vertical;
    return this;
  }

  public TableModelBuilder name(@NotNull String name) {
    this.name = name;
    return this;
  }

  public TableModelBuilder version(@NotNull String version) {
    this.version = version;
    return this;
  }

  public TableModelBuilder className(String className) {
    this.className = className;
    return this;
  }

  public TableModelBuilder parallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  public TableModelBuilder maxBulkSize(int maxBulkSize) {
    this.maxBulkSize = maxBulkSize;
    return this;
  }

  public TableModelBuilder partitionUnit(PartitionUnit partitionUnit) {
    this.partitionUnit = partitionUnit;
    return this;
  }

  public TableModelBuilder partitionSize(int partitionSize) {
    this.partitionSize = partitionSize;
    return this;
  }

  public TableModelBuilder partitioned(boolean partitioned) {
    this.partitioned = partitioned;
    return this;
  }

  public TableModelBuilder dependencies(DependencyModel[] dependencies) {
    this.dependencies = dependencies;
    return this;
  }

  public TableModelBuilder sources(SourceModel[] sources) {
    this.sources = sources;
    return this;
  }

  public TableModelBuilder targets(TargetModel[] targets) {
    this.targets = targets;
    return this;
  }

  public TableModel build() {
    return new TableModel(this);
  }

  @Override
  TableModelBuilder getThis() {
    return this;
  }
}
