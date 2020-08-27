package io.qimia.uhrwerk.config;

public class SourcePartitionBuilder extends PartitionBuilder<SourcePartitionBuilder> {
  private SourceBuilder parent;

  public SourcePartitionBuilder(SourceBuilder parent) {
    this.parent = parent;
  }

  public SourceBuilder done() {
    this.parent.partition(this.build());
    return this.parent;
  }
}
