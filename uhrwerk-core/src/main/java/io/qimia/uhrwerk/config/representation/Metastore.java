package io.qimia.uhrwerk.config.representation;

public class Metastore {
    private String jdbc_url;
    private String jdbc_driver;
    private String user;
    private String pass;

    public Metastore(String jdbc_url, String jdbc_driver, String user, String pass) {
        this.jdbc_url = jdbc_url;
        this.jdbc_driver = jdbc_driver;
        this.user = user;
        this.pass = pass;
    }

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
}
