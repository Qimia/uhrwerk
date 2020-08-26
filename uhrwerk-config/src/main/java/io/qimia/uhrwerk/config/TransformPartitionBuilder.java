package io.qimia.uhrwerk.config;

public class TransformPartitionBuilder extends PartitionBuilder<TransformPartitionBuilder> {
  private TransformBuilder parent;

  public TransformPartitionBuilder(TransformBuilder parent) {
    this.parent = parent;
  }

  public TransformBuilder done() {
    return this.parent;
  }
}
