package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.ConnectionModel;
import io.qimia.uhrwerk.config.representation.File;
import io.qimia.uhrwerk.config.representation.JDBC;
import io.qimia.uhrwerk.config.representation.S3;

public class ConnectionBuilder {
  private DagBuilder parent;
  private JDBCBuilder jdbcBuilder;
  private S3Builder s3Builder;
  private FileBuilder fileBuilder;

  private String name;
  private JDBC jdbc;
  private S3 s3;
  private File file;

  public ConnectionBuilder() {}

  public ConnectionBuilder(DagBuilder parent) {
    this.parent = parent;
  }

  public ConnectionBuilder name(String name) {
    this.name = name;
    return this;
  }

  public JDBCBuilder jdbc() {
    this.jdbcBuilder = new JDBCBuilder(this);
    return this.jdbcBuilder;
  }

  public ConnectionBuilder jdbc(JDBC jdbc) {
    this.jdbc = jdbc;
    return this;
  }

  public ConnectionBuilder jdbc(JDBCBuilder jdbcBuilder) {
    this.jdbc = jdbcBuilder.build();
    return this;
  }

  public FileBuilder file() {
    this.fileBuilder = new FileBuilder(this);
    return this.fileBuilder;
  }

  public ConnectionBuilder file(File file) {
    this.file = file;
    return this;
  }

  public ConnectionBuilder file(FileBuilder fileBuilder) {
    this.file = fileBuilder.build();
    return this;
  }

  public S3Builder s3() {
    this.s3Builder = new S3Builder(this);
    return this.s3Builder;
  }

  public ConnectionBuilder s3(S3 s3) {
    this.s3 = s3;
    return this;
  }

  public ConnectionBuilder s3(S3Builder s3Builder) {
    this.s3 = s3Builder.build();
    return this;
  }

  public DagBuilder done() {
    this.parent.connection(this.buildRepresentationConnection());
    return this.parent;
  }

  public io.qimia.uhrwerk.config.representation.Connection buildRepresentationConnection() {
    var connection = new io.qimia.uhrwerk.config.representation.Connection();
    connection.setName(this.name);
    connection.setS3(this.s3);
    connection.setFile(this.file);
    connection.setJdbc(this.jdbc);
    return connection;
  }

  public ConnectionModel build() {
    var connection = new io.qimia.uhrwerk.config.representation.Connection();
    connection.setName(this.name);
    connection.setS3(this.s3);
    connection.setFile(this.file);
    connection.setJdbc(this.jdbc);
    return ModelMapper.toConnectionFull(connection);
  }
}
