package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Partition;
import io.qimia.uhrwerk.config.representation.Transform;

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

  public DependencyBuilder done() {
    this.partition = this.partitionBuilder.build();
    return this.parent;
  }

  public Transform build() {
    Transform transform = new Transform();
    transform.setPartition(this.partitionBuilder.build());
    transform.setType(this.type);
    transform.validate("");
    return transform;
  }
}
