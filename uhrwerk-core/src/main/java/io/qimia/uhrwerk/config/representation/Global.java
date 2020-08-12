package io.qimia.uhrwerk.config.representation;

public class Global {
    private Uhrwerk uhrwerk;
    private Config config;

    public Global() {
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
