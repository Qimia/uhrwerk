package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.Metastore;

public class MetastoreBuilder {
    private io.qimia.uhrwerk.config.representation.Metastore metastore;

    public MetastoreBuilder() {
        this.metastore = new io.qimia.uhrwerk.config.representation.Metastore();
    }

    public MetastoreBuilder jdbc_url(String jdbc_url){
        this.metastore.setJdbc_url(jdbc_url);
        return this;
    }

    public MetastoreBuilder jdbc_driver(String jdbc_driver){
        this.metastore.setJdbc_driver(jdbc_driver);
        return this;
    }

    public MetastoreBuilder user(String user){
        this.metastore.setUser(user);
        return this;
    }

    public MetastoreBuilder pass(String pass){
        this.metastore.setPass(pass);
        return this;
    }

    public Metastore build(){
        YamlConfigReader configReader = new YamlConfigReader();
        return configReader.getModelMetastore(this.metastore);
    }
}
