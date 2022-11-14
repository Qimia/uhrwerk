package io.qimia.uhrwerk.common.model;

import org.jetbrains.annotations.NotNull;

public class TargetModelBuilder extends StateModelBuilder {

  Long id;
  Long tableId;
  Long connectionId;
  ConnectionModel connection;
  TableModel table;

  String format;
  boolean active = true;

  public TargetModelBuilder id(Long id) {
    this.id = id;
    return this;
  }

  public TargetModelBuilder tableId(@NotNull Long tableId) {
    this.tableId = tableId;
    return this;
  }

  public TargetModelBuilder connectionId(@NotNull Long connectionId) {
    this.connectionId = connectionId;
    return this;
  }

  public TargetModelBuilder connection(ConnectionModel connection) {
    this.connection = connection;
    return this;
  }

  public TargetModelBuilder table(TableModel table) {
    this.table = table;
    return this;
  }

  public TargetModelBuilder format(@NotNull String format) {
    this.format = format;
    return this;
  }

  public TargetModel build() {
    return new TargetModel(this);
  }

  @Override
  StateModelBuilder getThis() {
    return this;
  }
}
