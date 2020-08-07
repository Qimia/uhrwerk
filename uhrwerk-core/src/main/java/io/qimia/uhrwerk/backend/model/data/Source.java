package io.qimia.uhrwerk.backend.model.data;


public class Source {

  private long id;
  private long cfTableId;
  private long connectionId;
  private String sqlSelectQuery;
  private String sqlPartitionQuery;
  private String partitionColumn;
  private String queryColumn;
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


  public long getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(long connectionId) {
    this.connectionId = connectionId;
  }


  public String getSqlSelectQuery() {
    return sqlSelectQuery;
  }

  public void setSqlSelectQuery(String sqlSelectQuery) {
    this.sqlSelectQuery = sqlSelectQuery;
  }


  public String getSqlPartitionQuery() {
    return sqlPartitionQuery;
  }

  public void setSqlPartitionQuery(String sqlPartitionQuery) {
    this.sqlPartitionQuery = sqlPartitionQuery;
  }


  public String getPartitionColumn() {
    return partitionColumn;
  }

  public void setPartitionColumn(String partitionColumn) {
    this.partitionColumn = partitionColumn;
  }


  public String getQueryColumn() {
    return queryColumn;
  }

  public void setQueryColumn(String queryColumn) {
    this.queryColumn = queryColumn;
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
