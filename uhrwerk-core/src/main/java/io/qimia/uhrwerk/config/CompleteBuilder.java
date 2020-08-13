package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Config;
import io.qimia.uhrwerk.config.representation.Complete;
import io.qimia.uhrwerk.config.representation.Table;
import io.qimia.uhrwerk.config.representation.Uhrwerk;

public class CompleteBuilder {
    private Uhrwerk uhrwerk;
    private Config config;
    private Table[] tables;

    public CompleteBuilder withUhrwerk(Uhrwerk uhrwerk) {
        this.uhrwerk = uhrwerk;
        return this;
    }

    public CompleteBuilder withConfig(Config config) {
        this.config = config;
        return this;
    }

    public CompleteBuilder withTables(Table[] tables) {
        this.tables = tables;
        return this;
    }

    public Complete build(){
        Complete complete = new Complete();
        complete.setConfig(this.config);
        complete.setUhrwerk(this.uhrwerk);
        complete.setTables(this.tables);

        return complete;
    }
}
