package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Global;
import io.qimia.uhrwerk.config.representation.Complete;
import io.qimia.uhrwerk.config.representation.Table;

public class CompleteBuilder {
    private Global global;
    private Table[] tables;

    public CompleteBuilder withGlobal(Global global) {
        this.global = global;
        return this;
    }


    public CompleteBuilder withTables(Table[] tables) {
        this.tables = tables;
        return this;
    }

    public Complete build(){
        Complete complete = new Complete();
        complete.setGlobal(this.global);
        complete.setTables(this.tables);

        return complete;
    }
}
