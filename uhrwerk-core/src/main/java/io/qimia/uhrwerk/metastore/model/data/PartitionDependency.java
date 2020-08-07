package io.qimia.uhrwerk.metastore.model.data;


public class PartitionDependency {

  private long id;
  private long partitionId;
  private long dependencyPartitionId;
  private java.sql.Timestamp createdTs;
  private java.sql.Timestamp updatedTs;
  private String description;


  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }


  public long getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(long partitionId) {
    this.partitionId = partitionId;
  }


  public long getDependencyPartitionId() {
    return dependencyPartitionId;
  }

  public void setDependencyPartitionId(long dependencyPartitionId) {
    this.dependencyPartitionId = dependencyPartitionId;
  }


  public java.sql.Timestamp getCreatedTs() {
    return createdTs;
  }

  public void setCreatedTs(java.sql.Timestamp createdTs) {
    this.createdTs = createdTs;
  }


  public java.sql.Timestamp getUpdatedTs() {
    return updatedTs;
  }

  public void setUpdatedTs(java.sql.Timestamp updatedTs) {
    this.updatedTs = updatedTs;
  }


  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

}
