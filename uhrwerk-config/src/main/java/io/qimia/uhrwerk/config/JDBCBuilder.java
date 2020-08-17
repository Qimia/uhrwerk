package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.JDBC;

public class JDBCBuilder {
  private JDBC jdbc;

  public JDBCBuilder() {
    this.jdbc = new JDBC();
  }

  public JDBCBuilder url(String url) {
    this.jdbc.setJdbc_url(url);
    return this;
  }

  public JDBC build() {
    this.jdbc.validate("");
    return this.jdbc;
  }
}
