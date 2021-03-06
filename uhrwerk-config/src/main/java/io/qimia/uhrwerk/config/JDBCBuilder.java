package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.JDBC;

public class JDBCBuilder {
  private String jdbcUrl;
  private String jdbcDriver;
  private String user;
  private String pass;
  private ConnectionBuilder parent;

  public JDBCBuilder() {}

  public JDBCBuilder(ConnectionBuilder parent) {
    this.parent = parent;
  }

  public JDBCBuilder jdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
    return this;
  }

  public JDBCBuilder jdbcDriver(String jdbcDriver) {
    this.jdbcDriver = jdbcDriver;
    return this;
  }

  public JDBCBuilder user(String user) {
    this.user = user;
    return this;
  }

  public JDBCBuilder pass(String pass) {
    this.pass = pass;
    return this;
  }

  public ConnectionBuilder done() {
    this.parent.jdbc(this.build());
    return this.parent;
  }

  public JDBC build() {
    var jdbc = new JDBC();
    jdbc.setPass(this.pass);
    jdbc.setUser(this.user);
    jdbc.setJdbc_driver(this.jdbcDriver);
    jdbc.setJdbc_url(this.jdbcUrl);
    jdbc.validate("");
    return jdbc;
  }
}
