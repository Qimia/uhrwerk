package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.config.representation.Partition;
import io.qimia.uhrwerk.config.representation.Transform;
import io.qimia.uhrwerk.config.representation.TransformType;

public class TransformBuilder {
  private String type;
  private Partition partition;
  private DependencyBuilder parent;
  private TransformPartitionBuilder partitionBuilder;

  public TransformBuilder() {}

  public TransformBuilder(DependencyBuilder parent) {
    this.parent = parent;
  }

  public TransformBuilder type(String type) {
    this.type = type;
    return this;
  }

  public TransformPartitionBuilder partition() {
    this.partitionBuilder = new TransformPartitionBuilder(this);
    return this.partitionBuilder;
  }

  public TransformBuilder partition(Partition partition) {
    this.partition = partition;
    return this;
  }

  public TransformBuilder partition(PartitionBuilder partitionBuilder) {
    this.partition = partitionBuilder.build();
    return this;
  }

  public DependencyBuilder done() {
    this.parent.transform(this.build());
    return this.parent;
  }

  public Transform build() {
    Transform transform = new Transform();
    transform.setPartition(this.partition);
    transform.setType(TransformType.valueOf(this.type.toUpperCase()));
    transform.validate("");
    return transform;
  }
}
