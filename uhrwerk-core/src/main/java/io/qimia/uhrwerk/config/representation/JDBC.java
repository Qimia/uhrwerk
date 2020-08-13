package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

public class JDBC extends Representation{
    private String jdbc_url;
    private String jdbc_driver;
    private String user;
    private String pass;

    public JDBC(){}

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
    public void validate(String path){
        path += "jdbc/";
        if(jdbc_url == null){
            throw new ConfigException("Missing field: " + path + "jdbc_url");
        }
        if(jdbc_driver == null){
            throw new ConfigException("Missing field: " + path + "jdbc_driver");
        }
        if(user == null){
            throw new ConfigException("Missing field: " + path + "user");
        }
        if(pass == null){
            throw new ConfigException("Missing field: " + path + "pass");
        }
    }
}
