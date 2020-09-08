package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.Metastore;

public class OldMetastoreBuilder {
    private io.qimia.uhrwerk.config.representation.Metastore metastore;

    public OldMetastoreBuilder() {
        this.metastore = new io.qimia.uhrwerk.config.representation.Metastore();
    }

    public OldMetastoreBuilder jdbcUrl(String jdbcUrl){
        this.metastore.setJdbc_url(jdbcUrl);
        return this;
    }

    public OldMetastoreBuilder jdbcDriver(String jdbcDriver){
        this.metastore.setJdbc_driver(jdbcDriver);
        return this;
    }

    public OldMetastoreBuilder user(String user){
        this.metastore.setUser(user);
        return this;
    }

    public OldMetastoreBuilder pass(String pass){
        this.metastore.setPass(pass);
        return this;
    }

    public Metastore build(){
        YamlConfigReader configReader = new YamlConfigReader();
        return configReader.getModelMetastore(this.metastore);
    }
}
