package io.qimia.uhrwerk.models.db.data;

import io.qimia.uhrwerk.models.db.BatchTemporalUnit;
import io.qimia.uhrwerk.models.db.PartitionTransform;

public class DataDependency {

  private long id;
  private long cfTableId;
  private long dtTableId;
  private PartitionTransform partitionTransform;
  private BatchTemporalUnit batchTemporalUnit;
  private int batchSize;
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

  public long getCfTableId() {
    return cfTableId;
  }

  public void setCfTableId(long cfTableId) {
    this.cfTableId = cfTableId;
  }

  public long getDtTableId() {
    return dtTableId;
  }

  public void setDtTableId(long dtTableId) {
    this.dtTableId = dtTableId;
  }

  public PartitionTransform getPartitionTransform() {
    return partitionTransform;
  }

  public void setPartitionTransform(PartitionTransform partitionTransform) {
    this.partitionTransform = partitionTransform;
  }

  public BatchTemporalUnit getBatchTemporalUnit() {
    return batchTemporalUnit;
  }

  public void setBatchTemporalUnit(BatchTemporalUnit batchTemporalUnit) {
    this.batchTemporalUnit = batchTemporalUnit;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
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
