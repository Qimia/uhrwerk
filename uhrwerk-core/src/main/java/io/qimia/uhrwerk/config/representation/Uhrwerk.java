package io.qimia.uhrwerk.config.representation;

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

}
