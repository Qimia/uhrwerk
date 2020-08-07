package io.qimia.uhrwerk.backend.model.data;

public class Target {

  private long id;
  private long cfTableId;
  private long cfConnectionId;
  private String path;
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

  public long getCfConnectionId() {
    return cfConnectionId;
  }

  public void setCfConnectionId(long cfConnectionId) {
    this.cfConnectionId = cfConnectionId;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
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
