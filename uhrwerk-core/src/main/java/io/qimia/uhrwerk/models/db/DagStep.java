package com.sample;


public class DagStep {

  private long id;
  private long tableId;
  private long parallelism;
  private long maxPartitions;
  private String version;
  private java.sql.Timestamp createdTs;
  private java.sql.Timestamp updatedTs;
  private String description;


  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }


  public long getTableId() {
    return tableId;
  }

  public void setTableId(long tableId) {
    this.tableId = tableId;
  }


  public long getParallelism() {
    return parallelism;
  }

  public void setParallelism(long parallelism) {
    this.parallelism = parallelism;
  }


  public long getMaxPartitions() {
    return maxPartitions;
  }

  public void setMaxPartitions(long maxPartitions) {
    this.maxPartitions = maxPartitions;
  }


  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
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
