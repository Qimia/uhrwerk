package io.qimia.uhrwerk.backend.model.data;


public class Partition {

  private long id;
  private long dtTargetId;
  private String path;
  private String year;
  private String month;
  private String day;
  private String hour;
  private String minute;
  private String partitionHash;
  private java.sql.Timestamp createdTs;
  private java.sql.Timestamp updatedTs;
  private String description;


  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }


  public long getDtTargetId() {
    return dtTargetId;
  }

  public void setDtTargetId(long dtTargetId) {
    this.dtTargetId = dtTargetId;
  }


  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }


  public String getYear() {
    return year;
  }

  public void setYear(String year) {
    this.year = year;
  }


  public String getMonth() {
    return month;
  }

  public void setMonth(String month) {
    this.month = month;
  }


  public String getDay() {
    return day;
  }

  public void setDay(String day) {
    this.day = day;
  }


  public String getHour() {
    return hour;
  }

  public void setHour(String hour) {
    this.hour = hour;
  }


  public String getMinute() {
    return minute;
  }

  public void setMinute(String minute) {
    this.minute = minute;
  }


  public String getPartitionHash() {
    return partitionHash;
  }

  public void setPartitionHash(String partitionHash) {
    this.partitionHash = partitionHash;
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