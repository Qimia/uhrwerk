package io.qimia.uhrwerk.config.representation;

public class Global {
    private Uhrwerk uhrwerk;
    private Config config;

    public Global(Uhrwerk uhrwerk, Config config) {
        this.uhrwerk = uhrwerk;
        this.config = config;
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
}
