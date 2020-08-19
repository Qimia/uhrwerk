package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;

public class ConnectionBuilder {
  private Connection connection;

  public ConnectionBuilder() {
    this.connection = connection;
  }

  public ConnectionBuilder name(String name) {
    this.connection.setName(name);
    return this;
  }

  public ConnectionBuilder type(ConnectionType type) {
    this.connection.setType(type);
    return this;
  }
  public ConnectionBuilder path(String path) {
    this.connection.setPath(path);
    return this;
  }
  public ConnectionBuilder jdbcUrl(String jdbcUrl) {
    this.connection.setJdbcUrl(jdbcUrl);
    return this;
  }
  public ConnectionBuilder jdbcDriver(String jdbcDriver) {
    this.connection.setJdbcDriver(jdbcDriver);
    return this;
  }
  public ConnectionBuilder jdbcUser(String jdbcUser) {
    this.connection.setJdbcUser(jdbcUser);
    return this;
  }
  public ConnectionBuilder jdbcPass(String jdbcPass) {
    this.connection.setJdbcPass(jdbcPass);
    return this;
  }
  public ConnectionBuilder awsAccessKeyID(String awsAccessKeyID) {
    this.connection.setAwsSecretAccessKey(awsAccessKeyID);
    return this;
  }
  public ConnectionBuilder awsSecretAccessKey(String awsSecretAccessKey) {
    this.connection.setAwsSecretAccessKey(awsSecretAccessKey);
    return this;
  }

  public Connection build() {
    //this.connection.validate("");
    return this.connection;
  }
}
