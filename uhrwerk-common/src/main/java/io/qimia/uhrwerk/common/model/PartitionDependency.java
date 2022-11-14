package io.qimia.uhrwerk.common.model;

import com.google.common.base.Objects;

public class PartitionDependency extends BaseModel {
  Long partitionId;
  Long dependencyPartitionId;

  public static PartitionDependencyBuilder builder() {
    return new PartitionDependencyBuilder();
  }

  public PartitionDependency(PartitionDependencyBuilder builder) {
    this.id = builder.id;
    this.partitionId = builder.partitionId;
    this.dependencyPartitionId = builder.dependencyPartitionId;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(Long partitionId) {
    this.partitionId = partitionId;
  }

  public Long getDependencyPartitionId() {
    return dependencyPartitionId;
  }

  public void setDependencyPartitionId(Long dependencyPartitionId) {
    this.dependencyPartitionId = dependencyPartitionId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionDependency)) {
      return false;
    }
    PartitionDependency that = (PartitionDependency) o;
    return Objects.equal(getId(), that.getId())
        && Objects.equal(getPartitionId(), that.getPartitionId())
        && Objects.equal(getDependencyPartitionId(), that.getDependencyPartitionId());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getId(), getPartitionId(), getDependencyPartitionId());
  }

  @Override
  public String toString() {
    return "PartitionDependency{"
        + "id="
        + id
        + ", partitionId="
        + partitionId
        + ", dependencyPartitionId="
        + dependencyPartitionId
        + '}';
  }
}
