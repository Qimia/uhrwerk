package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

public class Global extends Representation{
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

    @Override
    public void validate(String path){
        path += "global/";
        if(uhrwerk==null){
            throw new ConfigException("Missing field:" + path + "uhrwerk");
        }
        else{
            uhrwerk.validate(path);
        }
        if(config==null){
            throw new ConfigException("Missing field:" + path + "config");
        }
        else{
            config.validate(path);
        }
    }
}
