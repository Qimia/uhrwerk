package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

public class Uhrwerk {
    private Metastore metastore;

    public Uhrwerk() {
    }

    public Metastore getMetastore() {
        return metastore;
    }

    public void setMetastore(Metastore metastore) {
        this.metastore = metastore;
    }

    public void validate(String path){
        path += "uhrwerk/";
        if(metastore == null){
            throw new ConfigException("Missing field: " + path + "metastore");
        }
    }
}
