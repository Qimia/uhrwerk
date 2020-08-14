package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

public class Uhrwerk extends Representation{
    private Metastore metastore;

    public Uhrwerk() {
    }

    public Metastore getMetastore() {
        return metastore;
    }

    public void setMetastore(Metastore metastore) {
        this.metastore = metastore;
    }

    @Override
    public void validate(String path){
        path += "metastore/";
        if(metastore == null){
            throw new ConfigException("Missing field: " + path + "metastore");
        }
        else {
            metastore.validate(path);
        }
    }
}
