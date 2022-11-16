package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel;
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
    if (this.s3 != null) {
      this.s3.setName(this.name);
      return this.s3;
    }
    if (this.jdbc != null) {
      this.jdbc.setName(this.name);
      return this.jdbc;
    }
    if (file != null) {
      this.file.setName(name);
      return this.file;
    }
    return null;
  }

  public ConnectionModel build() {
    io.qimia.uhrwerk.config.representation.Connection connection = null;
    if (this.s3 != null) {
      this.s3.setName(this.name);
      connection = this.s3;
    }
    if (this.jdbc != null) {
      this.jdbc.setName(this.name);
      connection = this.jdbc;
    }
    if (file != null) {
      this.file.setName(name);
      connection = this.file;
    }
    return ModelMapper.toConnectionFull(connection);
  }
}
