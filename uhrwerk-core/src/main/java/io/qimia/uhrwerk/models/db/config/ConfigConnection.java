package io.qimia.uhrwerk.models.db.config;

public class ConfigConnection {

  private long id;
  private String connectionName;
  private String connectionType;
  private String connectionUrl;
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

  public String getConnectionName() {
    return connectionName;
  }

  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  public String getConnectionType() {
    return connectionType;
  }

  public void setConnectionType(String connectionType) {
    this.connectionType = connectionType;
  }

  public String getConnectionUrl() {
    return connectionUrl;
  }

  public void setConnectionUrl(String connectionUrl) {
    this.connectionUrl = connectionUrl;
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
