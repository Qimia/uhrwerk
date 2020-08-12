package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.JDBC;

public class JDBCBuilder {
    private String jdbcUrl;
    private String jdbcDriver;
    private String user;
    private String pass;

    public JDBCBuilder withJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public JDBCBuilder withJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
        return this;
    }

    public JDBCBuilder withUser(String user) {
        this.user = user;
        return this;
    }

    public JDBCBuilder withPass(String pass) {
        this.pass = pass;
        return this;
    }

    public JDBC build(){
        return new JDBC();
    }
}
