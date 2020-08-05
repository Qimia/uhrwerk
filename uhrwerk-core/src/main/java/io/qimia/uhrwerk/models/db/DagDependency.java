package io.qimia.uhrwerk.models.db;


public class DagDependency {

  private long id;
  private long tableId;
  private long stepId;
  private String partitionTransform;
  private String batchTemporalUnit;
  private long batchSize;
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


  public long getStepId() {
    return stepId;
  }

  public void setStepId(long stepId) {
    this.stepId = stepId;
  }


  public String getPartitionTransform() {
    return partitionTransform;
  }

  public void setPartitionTransform(String partitionTransform) {
    this.partitionTransform = partitionTransform;
  }


  public String getBatchTemporalUnit() {
    return batchTemporalUnit;
  }

  public void setBatchTemporalUnit(String batchTemporalUnit) {
    this.batchTemporalUnit = batchTemporalUnit;
  }


  public long getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(long batchSize) {
    this.batchSize = batchSize;
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
