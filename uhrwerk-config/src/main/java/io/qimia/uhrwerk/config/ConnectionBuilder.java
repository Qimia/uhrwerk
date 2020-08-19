package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.File;
import io.qimia.uhrwerk.config.representation.S3;
import io.qimia.uhrwerk.config.representation.JDBC;

public class ConnectionBuilder {
  private io.qimia.uhrwerk.config.representation.Connection connection;
  private io.qimia.uhrwerk.config.representation.JDBC jdbc;
  private io.qimia.uhrwerk.config.representation.S3 s3;
  private io.qimia.uhrwerk.config.representation.File file;

  public ConnectionBuilder() {
    this.connection = new io.qimia.uhrwerk.config.representation.Connection();
  }

  public ConnectionBuilder name(String name) {
    this.connection.setName(name);
    return this;
  }

  public ConnectionBuilder jdbc(JDBC jdbc) {
    this.connection.setJdbc(jdbc);
    return this;
  }

  public ConnectionBuilder jdbc_url(String url) {
    if (this.jdbc == null) {
      this.jdbc = new JDBC();
    }
    this.jdbc.setJdbc_url(url);
    return this;
  }

  public ConnectionBuilder jdbc_driver(String driver) {
    if (this.jdbc == null) {
      this.jdbc = new JDBC();
    }
    this.jdbc.setJdbc_driver(driver);
    return this;
  }

  public ConnectionBuilder jdbc_user(String user) {
    if (this.jdbc == null) {
      this.jdbc = new JDBC();
    }
    this.jdbc.setUser(user);
    return this;
  }

  public ConnectionBuilder jdbc_pass(String pass) {
    if (this.jdbc == null) {
      this.jdbc = new JDBC();
    }
    this.jdbc.setPass(pass);
    return this;
  }

  public ConnectionBuilder s3(S3 s3) {
    this.connection.setS3(s3);
    return this;
  }

  public ConnectionBuilder s3_path(String path) {
    if (this.s3 == null) {
      this.s3 = new S3();
    }
    this.s3.setPath(path);
    return this;
  }

  public ConnectionBuilder s3_secret_id(String secret_id) {
    if (this.s3 == null) {
      this.s3 = new S3();
    }
    this.s3.setSecret_id(secret_id);
    return this;
  }

  public ConnectionBuilder s3_secret_key(String secret_key) {
    if (this.s3 == null) {
      this.s3 = new S3();
    }
    this.s3.setSecret_key(secret_key);
    return this;
  }

  public ConnectionBuilder file(File file) {
    this.connection.setFile(file);
    return this;
  }

  public ConnectionBuilder file_path(String path) {
    if (this.file == null) {
      this.file = new File();
    }
    this.file.setPath(path);
    return this;
  }

  public io.qimia.uhrwerk.common.model.Connection build() {
    if (this.jdbc != null){this.connection.setJdbc(this.jdbc);}
    if (this.s3 != null){this.connection.setS3(this.s3);}
    if (this.file != null){this.connection.setFile(this.file);}
    YamlConfigReader configReader = new YamlConfigReader();
    return configReader.getModelConnection(this.connection);
  }
}
