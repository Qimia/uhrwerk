package io.qimia.uhrwerk.common.model;

public class PartitionDependencyBuilder {

  Long id;
  Long partitionId;
  Long dependencyPartitionId;

  public PartitionDependencyBuilder id(Long id) {
    this.id = id;
    return this;
  }

  public PartitionDependencyBuilder partitionId(Long partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  public PartitionDependencyBuilder dependencyPartitionId(Long dependencyPartitionId) {
    this.dependencyPartitionId = dependencyPartitionId;
    return this;
  }

  public PartitionDependency build() {
    return new PartitionDependency(this);
  }
}
