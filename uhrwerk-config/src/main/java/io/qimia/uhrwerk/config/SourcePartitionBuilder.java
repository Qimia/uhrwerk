package io.qimia.uhrwerk.config;

public class SourcePartitionBuilder extends PartitionBuilder<SourcePartitionBuilder> {
  private SourceBuilder parent;

  public SourcePartitionBuilder(SourceBuilder parent) {
    this.parent = parent;
  }

  public SourceBuilder done() {
    return this.parent;
  }
}
