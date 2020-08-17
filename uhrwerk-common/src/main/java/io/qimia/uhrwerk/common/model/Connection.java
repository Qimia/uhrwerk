package io.qimia.uhrwerk.common.model;

import java.util.Objects;

public class Connection {

  Long id;
  String name;
  ConnectionType type;
  String path;
  String jdbcUrl;
  String jdbcDriver;
  String jdbcUser;
  String jdbcPass;
  String awsAccessKeyID;
  String awsSecretAccessKey;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ConnectionType getType() {
    return type;
  }

  public void setType(ConnectionType type) {
    this.type = type;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public String getJdbcDriver() {
    return jdbcDriver;
  }

  public void setJdbcDriver(String jdbcDriver) {
    this.jdbcDriver = jdbcDriver;
  }

  public String getJdbcUser() {
    return jdbcUser;
  }

  public void setJdbcUser(String jdbcUser) {
    this.jdbcUser = jdbcUser;
  }

  public String getJdbcPass() {
    return jdbcPass;
  }

  public void setJdbcPass(String jdbcPass) {
    this.jdbcPass = jdbcPass;
  }

  public String getAwsAccessKeyID() {
    return awsAccessKeyID;
  }

  public void setAwsAccessKeyID(String awsAccessKeyID) {
    this.awsAccessKeyID = awsAccessKeyID;
  }

  public String getAwsSecretAccessKey() {
    return awsSecretAccessKey;
  }

  public void setAwsSecretAccessKey(String awsSecretAccessKey) {
    this.awsSecretAccessKey = awsSecretAccessKey;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Connection that = (Connection) o;
    return Objects.equals(id, that.id) &&
            Objects.equals(name, that.name) &&
            type == that.type &&
            Objects.equals(path, that.path) &&
            Objects.equals(jdbcUrl, that.jdbcUrl) &&
            Objects.equals(jdbcDriver, that.jdbcDriver) &&
            Objects.equals(jdbcUser, that.jdbcUser) &&
            Objects.equals(jdbcPass, that.jdbcPass) &&
            Objects.equals(awsAccessKeyID, that.awsAccessKeyID) &&
            Objects.equals(awsSecretAccessKey, that.awsSecretAccessKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, type, path, jdbcUrl, jdbcDriver, jdbcUser, jdbcPass, awsAccessKeyID, awsSecretAccessKey);
  }

  @Override
  public String toString() {
    return "Connection{" +
            "id=" + id +
            ", name='" + name + '\'' +
            ", type=" + type +
            ", path='" + path + '\'' +
            ", jdbcUrl='" + jdbcUrl + '\'' +
            ", jdbcDriver='" + jdbcDriver + '\'' +
            ", jdbcUser='" + jdbcUser + '\'' +
            ", jdbcPass='" + jdbcPass + '\'' +
            ", awsAccessKeyID='" + awsAccessKeyID + '\'' +
            ", awsSecretAccessKey='" + awsSecretAccessKey + '\'' +
            '}';
  }
}
