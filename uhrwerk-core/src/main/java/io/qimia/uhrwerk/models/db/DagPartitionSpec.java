package io.qimia.uhrwerk.models.db;


public class DagPartitionSpec {

  private long id;
  private long tableInfoId;
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


  public long getTableInfoId() {
    return tableInfoId;
  }

  public void setTableInfoId(long tableInfoId) {
    this.tableInfoId = tableInfoId;
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
