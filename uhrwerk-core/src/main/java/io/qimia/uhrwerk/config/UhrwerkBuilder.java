package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Metastore;
import io.qimia.uhrwerk.config.representation.Uhrwerk;

public class UhrwerkBuilder {
    private Metastore metastore;

    public UhrwerkBuilder withMetastore(Metastore metastore) {
        this.metastore = metastore;
        return this;
    }

    public Uhrwerk build(){
        Uhrwerk uhrwerk = new Uhrwerk();
        uhrwerk.setMetastore(this.metastore);
        return uhrwerk;
    }
}
