package io.qimia.uhrwerk.config;

public class TablePartitionBuilder extends PartitionBuilder<TablePartitionBuilder> {
  private final TableBuilder parent;

  public TablePartitionBuilder(TableBuilder parent) {
    this.parent = parent;
  }

  public TableBuilder done() {
    this.parent.partition(this.build());
    return this.parent;
  }
}
