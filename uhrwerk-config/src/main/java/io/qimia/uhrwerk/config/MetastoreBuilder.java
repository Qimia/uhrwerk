package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Metastore;

public class MetastoreBuilder {
    private String jdbcUrl;
    private String jdbcDriver;
    private String user;
    private String pass;

    public MetastoreBuilder() {}


    public MetastoreBuilder jdbcUrl(String jdbcUrl){
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public MetastoreBuilder jdbcDriver(String jdbcDriver){
        this.jdbcDriver = jdbcDriver;
        return this;
    }

    public MetastoreBuilder user(String user){
        this.user = user;
        return this;
    }

    public MetastoreBuilder pass(String pass){
        this.pass = pass;
        return this;
    }

    public io.qimia.uhrwerk.common.model.Metastore build(){
        var metastore = new Metastore();
        metastore.setJdbc_driver(this.jdbcDriver);
        metastore.setJdbc_url(this.jdbcUrl);
        metastore.setUser(this.user);
        metastore.setPass(this.pass);
        YamlConfigReader configReader = new YamlConfigReader();
        return configReader.getModelMetastore(metastore);
    }
}
