package io.qimia.uhrwerk.common.model;

import java.sql.Timestamp;

public abstract class BaseModel {
  Long id;
  private Timestamp createdTs;
  private Timestamp updatedTs;

  public Timestamp getCreatedTs() {
    return createdTs;
  }

  public void setCreatedTs(Timestamp createdTs) {
    this.createdTs = createdTs;
  }

  public Timestamp getUpdatedTs() {
    return updatedTs;
  }

  public void setUpdatedTs(Timestamp updatedTs) {
    this.updatedTs = updatedTs;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }
}
