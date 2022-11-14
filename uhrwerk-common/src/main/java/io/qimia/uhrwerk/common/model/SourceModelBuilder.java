package io.qimia.uhrwerk.common.model;

import org.jetbrains.annotations.NotNull;

public class SourceModelBuilder extends StateModelBuilder<SourceModelBuilder> {

  Long id;
  Long tableId;
  Long connectionId;
  String path;
  String format;
  ConnectionModel connection;
  TableModel table;
  PartitionUnit partitionUnit;
  int partitionSize;
  String parallelLoadQuery;
  String parallelLoadColumn;
  int parallelLoadNum;
  String selectQuery;
  String selectColumn;
  boolean partitioned;
  boolean autoLoad;

  public SourceModelBuilder id(Long id) {
    this.id = id;
    return this;
  }

  public SourceModelBuilder tableId(@NotNull Long tableId) {
    this.tableId = tableId;
    return this;
  }

  public SourceModelBuilder connectionId(@NotNull Long connectionId) {
    this.connectionId = connectionId;
    return this;
  }

  public SourceModelBuilder path(@NotNull String path) {
    this.path = path;
    return this;
  }

  public SourceModelBuilder format(@NotNull String format) {
    this.format = format;
    return this;
  }

  public SourceModelBuilder connection(ConnectionModel connection) {
    this.connection = connection;
    return this;
  }

  public SourceModelBuilder table(TableModel table) {
    this.table = table;
    return this;
  }

  public SourceModelBuilder partitionUnit(PartitionUnit partitionUnit) {
    this.partitionUnit = partitionUnit;
    return this;
  }

  public SourceModelBuilder partitionSize(int partitionSize) {
    this.partitionSize = partitionSize;
    return this;
  }

  public SourceModelBuilder parallelLoadQuery(String parallelLoadQuery) {
    this.parallelLoadQuery = parallelLoadQuery;
    return this;
  }

  public SourceModelBuilder parallelLoadColumn(String parallelLoadColumn) {
    this.parallelLoadColumn = parallelLoadColumn;
    return this;
  }

  public SourceModelBuilder parallelLoadNum(int parallelLoadNum) {
    this.parallelLoadNum = parallelLoadNum;
    return this;
  }

  public SourceModelBuilder selectQuery(String selectQuery) {
    this.selectQuery = selectQuery;
    return this;
  }

  public SourceModelBuilder selectColumn(String selectColumn) {
    this.selectColumn = selectColumn;
    return this;
  }

  public SourceModelBuilder partitioned(boolean partitioned) {
    this.partitioned = partitioned;
    return this;
  }

  public SourceModelBuilder autoLoad(boolean autoLoad) {
    this.autoLoad = autoLoad;
    return this;
  }

  public SourceModel build() {
    return new SourceModel(this);
  }


  @Override
  SourceModelBuilder getThis() {
    return this;
  }
}
