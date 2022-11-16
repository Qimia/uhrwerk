package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.common.metastore.model.MetastoreModel;
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

    public MetastoreModel build(){
        var metastore = new Metastore();
        metastore.setJdbcDriver(this.jdbcDriver);
        metastore.setJdbcUrl(this.jdbcUrl);
        metastore.setUser(this.user);
        metastore.setPassword(this.pass);
        YamlConfigReader configReader = new YamlConfigReader();
        return configReader.getModelMetastore(metastore);
    }
}
