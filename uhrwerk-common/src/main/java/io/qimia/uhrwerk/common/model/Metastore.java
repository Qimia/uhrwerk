package io.qimia.uhrwerk.common.model;

import java.io.Serializable;
import java.util.Objects;

public class Metastore implements Serializable {

  private static final long serialVersionUID = -7850907844323874344L;

  private String jdbc_url;
  private String jdbc_driver;
  private String user;
  private String pass;

  public String getJdbc_url() {
    return jdbc_url;
  }

  public void setJdbc_url(String jdbc_url) {
    this.jdbc_url = jdbc_url;
  }

  public String getJdbc_driver() {
    return jdbc_driver;
  }

  public void setJdbc_driver(String jdbc_driver) {
    this.jdbc_driver = jdbc_driver;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPass() {
    return pass;
  }

  public void setPass(String pass) {
    this.pass = pass;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Metastore metastore = (Metastore) o;
    return Objects.equals(jdbc_url, metastore.jdbc_url)
        && Objects.equals(jdbc_driver, metastore.jdbc_driver)
        && Objects.equals(user, metastore.user)
        && Objects.equals(pass, metastore.pass);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jdbc_url, jdbc_driver, user, pass);
  }

  @Override
  public String toString() {
    return "Metastore{"
        + "jdbc_url='"
        + jdbc_url
        + '\''
        + ", jdbc_driver='"
        + jdbc_driver
        + '\''
        + ", user='"
        + user
        + '\''
        + ", pass='"
        + pass
        + '\''
        + '}';
  }
}
