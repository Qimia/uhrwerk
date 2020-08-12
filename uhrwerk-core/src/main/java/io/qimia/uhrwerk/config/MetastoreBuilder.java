package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Metastore;

public class MetastoreBuilder {
    private String jdbcUrl;
    private String jdbcDriver;
    private String user;
    private String pass;

    public MetastoreBuilder withJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public MetastoreBuilder withJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
        return this;
    }

    public MetastoreBuilder withUser(String user) {
        this.user = user;
        return this;
    }

    public MetastoreBuilder withPass(String pass) {
        this.pass = pass;
        return this;
    }

    public Metastore build(){
        return new Metastore();
    }
}
