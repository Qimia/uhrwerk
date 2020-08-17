package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.File;
import io.qimia.uhrwerk.config.representation.JDBC;
import io.qimia.uhrwerk.config.representation.S3;

public class ConnectionBuilder {
  private Connection connection;

  public ConnectionBuilder() {
    this.connection = connection;
  }

  public ConnectionBuilder name(String name) {
    this.connection.setName(name);
    return this;
  }

  public ConnectionBuilder withJdbc(JDBC jdbc) {
    return this;
  }

  public ConnectionBuilder withS3(S3 s3) {
    return this;
  }

  public ConnectionBuilder withFile(File file) {
    return this;
  }

  public Connection build() {
    this.connection.validate("");
    return this.connection;
  }
}
