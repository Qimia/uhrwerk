package io.qimia.uhrwerk.models.db.config;

import io.qimia.uhrwerk.models.db.BatchTemporalUnit;

public class ConfigTable {

  private long id;
  private String area;
  private String vertical;
  private String tableName;
  private BatchTemporalUnit batchTemporalUnit;
  private int batchSize;
  private int parallelism;
  private int maxPartitions;
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

  public String getArea() {
    return area;
  }

  public void setArea(String area) {
    this.area = area;
  }

  public String getVertical() {
    return vertical;
  }

  public void setVertical(String vertical) {
    this.vertical = vertical;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
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

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public int getMaxPartitions() {
    return maxPartitions;
  }

  public void setMaxPartitions(int maxPartitions) {
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
