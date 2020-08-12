package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.model.Global;
import io.qimia.uhrwerk.config.representation.Config;
import io.qimia.uhrwerk.config.representation.Uhrwerk;

public class GlobalBuilder {
    private Uhrwerk uhrwerk;
    private Config config;

    public GlobalBuilder withUhrwerk(Uhrwerk uhrwerk) {
        this.uhrwerk = uhrwerk;
        return this;
    }

    public GlobalBuilder withConfig(Config config) {
        this.config = config;
        return this;
    }

    public Global build(){
        return new Global();
    }
}
