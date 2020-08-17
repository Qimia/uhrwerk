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
        Metastore metastore = new Metastore();
        metastore.setJdbc_driver(this.jdbcDriver);
        metastore.setJdbc_url(this.jdbcUrl);
        metastore.setPass(this.pass);
        metastore.setUser(this.user);
        return metastore;
    }
}
