package io.qimia.uhrwerk.common.model;

import com.google.common.base.Objects;
import java.io.Serializable;

public class ConnectionModel extends StateModel implements Serializable {

  private static final long serialVersionUID = -3946000637155224648L;

  String name;
  ConnectionType type;
  String path;
  String jdbcUrl;
  String jdbcDriver;
  String jdbcUser;
  String jdbcPass;
  String awsAccessKeyID;
  String awsSecretAccessKey;

  public static ConnectionModelBuilder builder() {
    return new ConnectionModelBuilder();
  }

  public ConnectionModel(ConnectionModelBuilder builder) {
    super(builder.deactivatedTs);
    this.id = builder.id;
    this.name = builder.name;
    this.type = builder.type;
    this.path = builder.path;
    this.jdbcUrl = builder.jdbcUrl;
    this.jdbcDriver = builder.jdbcDriver;
    this.jdbcUser = builder.jdbcUser;
    this.jdbcPass = builder.jdbcPass;
    this.awsAccessKeyID = builder.awsAccessKeyID;
    this.awsSecretAccessKey = builder.awsSecretAccessKey;
  }

  public String getName() {
    return name;
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
    if (this == o) {
      return true;
    }
    if (!(o instanceof ConnectionModel)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ConnectionModel that = (ConnectionModel) o;
    return Objects.equal(getName(), that.getName())
        && getType() == that.getType()
        && Objects.equal(getPath(), that.getPath())
        && Objects.equal(getJdbcUrl(), that.getJdbcUrl())
        && Objects.equal(getJdbcDriver(), that.getJdbcDriver())
        && Objects.equal(getJdbcUser(), that.getJdbcUser())
        && Objects.equal(getJdbcPass(), that.getJdbcPass())
        && Objects.equal(getAwsAccessKeyID(), that.getAwsAccessKeyID())
        && Objects.equal(getAwsSecretAccessKey(), that.getAwsSecretAccessKey());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(),
        getName(),
        getType(),
        getPath(),
        getJdbcUrl(),
        getJdbcDriver(),
        getJdbcUser(),
        getJdbcPass(),
        getAwsAccessKeyID(),
        getAwsSecretAccessKey());
  }

  @Override
  public String toString() {
    return "ConnectionModel{"
        + "name='"
        + name
        + '\''
        + ", type="
        + type
        + ", path='"
        + path
        + '\''
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
        + '}';
  }
}
