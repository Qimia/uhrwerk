package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.JDBC;

public class ConfigJDBCBuilder {
  private JDBC jdbc;

  public ConfigJDBCBuilder() {
    this.jdbc = new JDBC();
  }

  public ConfigJDBCBuilder url(String url) {
    this.jdbc.setJdbc_url(url);
    return this;
  }

  public ConfigJDBCBuilder driver(String driver) {
    this.jdbc.setJdbc_driver(driver);
    return this;
  }

  public ConfigJDBCBuilder user(String user) {
    this.jdbc.setUser(user);
    return this;
  }

  public ConfigJDBCBuilder pass(String pass) {
    this.jdbc.setPass(pass);
    return this;
  }

  public JDBC build() {
    this.jdbc.validate("");
    return this.jdbc;
  }
}
