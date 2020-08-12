package io.qimia.uhrwerk.config.representation;

public class Uhrwerk {
    private Metastore metastore;

    public Uhrwerk(Metastore metastore) {
        this.metastore = metastore;
    }

    public Metastore getMetastore() {
        return metastore;
    }

    public void setMetastore(Metastore metastore) {
        this.metastore = metastore;
    }
}
