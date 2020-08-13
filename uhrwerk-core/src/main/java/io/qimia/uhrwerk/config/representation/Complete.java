package io.qimia.uhrwerk.config.representation;

public class Complete {
    private Uhrwerk uhrwerk;
    private Config config;
    private Table[] tables;

    public Complete() {
    }

    public Uhrwerk getUhrwerk() {
        return uhrwerk;
    }

    public void setUhrwerk(Uhrwerk uhrwerk) {
        this.uhrwerk = uhrwerk;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public Table[] getTables() {
        return tables;
    }

    public void setTables(Table[] tables) {
        this.tables = tables;
    }
}
