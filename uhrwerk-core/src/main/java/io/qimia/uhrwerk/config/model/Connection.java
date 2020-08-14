package io.qimia.uhrwerk.config.model;

import io.qimia.uhrwerk.config.ConnectionType;

import java.util.Objects;

public class Connection {

  String name;
  ConnectionType type;
  String path;
  String jdbcUrl;
  String jdbcDriver;
  String jdbcUser;
  String jdbcPass;
  String awsAccessKeyID;
  String awsSecretAccessKey;
  String version;

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

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Connection that = (Connection) o;
    return name.equals(that.name)
        && type == that.type
        && Objects.equals(jdbcUrl, that.jdbcUrl)
        && Objects.equals(jdbcDriver, that.jdbcDriver)
        && Objects.equals(jdbcUser, that.jdbcUser)
        && Objects.equals(jdbcPass, that.jdbcPass)
        && Objects.equals(awsAccessKeyID, that.awsAccessKeyID)
        && Objects.equals(awsSecretAccessKey, that.awsSecretAccessKey)
        && path.equals(that.path)
        && version.equals(that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        type,
        jdbcUrl,
        jdbcDriver,
        jdbcUser,
        jdbcPass,
        awsAccessKeyID,
        awsSecretAccessKey,
        path,
        version);
  }

  @Override
  public String toString() {
    return "Connection{"
        + "name='"
        + name
        + '\''
        + ", type="
        + type
        + ", jdbcUrl='"
        + jdbcUrl
        + '\''
        + ", jdbcDriver='"
        + jdbcDriver
        + '\''
        + ", jdbcUser='"
        + jdbcUser
        + '\''
        + ", jdbcPass='"
        + jdbcPass
        + '\''
        + ", awsAccessKeyID='"
        + awsAccessKeyID
        + '\''
        + ", awsSecretAccessKey='"
        + awsSecretAccessKey
        + '\''
        + ", path='"
        + path
        + '\''
        + ", version='"
        + version
        + '\''
        + '}';
  }
}
