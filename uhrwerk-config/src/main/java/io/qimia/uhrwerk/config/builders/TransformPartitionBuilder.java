package io.qimia.uhrwerk.config.builders;

public class TransformPartitionBuilder extends PartitionBuilder<TransformPartitionBuilder> {
  private final TransformBuilder parent;

  public TransformPartitionBuilder(TransformBuilder parent) {
    this.parent = parent;
  }

  public TransformBuilder done() {
    this.parent.partition(this.build());
    return this.parent;
  }
}
