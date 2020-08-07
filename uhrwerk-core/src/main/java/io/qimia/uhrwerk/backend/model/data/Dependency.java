package io.qimia.uhrwerk.backend.model.data;

import io.qimia.uhrwerk.backend.model.BatchTemporalUnit;
import io.qimia.uhrwerk.backend.model.PartitionTransform;

public class Dependency {

  private long id;
  private long cfTableId;
  private long dtTargetId;
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

  public long getDtTargetId() {
    return dtTargetId;
  }

  public void setDtTargetId(long dtTargetId) {
    this.dtTargetId = dtTargetId;
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
