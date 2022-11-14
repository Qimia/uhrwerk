package io.qimia.uhrwerk.common.model;

import org.jetbrains.annotations.NotNull;

public class ConnectionModelBuilder extends StateModelBuilder<ConnectionModelBuilder> {

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

  public ConnectionModelBuilder id(Long id) {
    this.id = id;
    return this;
  }

  public ConnectionModelBuilder name(@NotNull String name) {
    this.name = name;
    return this;
  }

  public ConnectionModelBuilder type(ConnectionType type) {
    this.type = type;
    return this;
  }

  public ConnectionModelBuilder path(String path) {
    this.path = path;
    return this;
  }

  public ConnectionModelBuilder jdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
    return this;
  }

  public ConnectionModelBuilder jdbcDriver(String jdbcDriver) {
    this.jdbcDriver = jdbcDriver;
    return this;
  }

  public ConnectionModelBuilder jdbcUser(String jdbcUser) {
    this.jdbcUser = jdbcUser;
    return this;
  }

  public ConnectionModelBuilder jdbcPass(String jdbcPass) {
    this.jdbcPass = jdbcPass;
    return this;
  }

  public ConnectionModelBuilder awsAccessKeyID(String awsAccessKeyID) {
    this.awsAccessKeyID = awsAccessKeyID;
    return this;
  }

  public ConnectionModelBuilder awsSecretAccessKey(String awsSecretAccessKey) {
    this.awsSecretAccessKey = awsSecretAccessKey;
    return this;
  }

  public ConnectionModel build() {
    return new ConnectionModel(this);
  }

  @Override
  ConnectionModelBuilder getThis() {
    return this;
  }
}
