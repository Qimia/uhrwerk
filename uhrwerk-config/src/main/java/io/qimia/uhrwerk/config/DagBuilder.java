package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;

public class DagBuilder {
    private Dag dag;

    public DagBuilder() { this.dag = new Dag(); }

    public DagBuilder connection(Connection[] connections) {
        this.dag.setConnections(connections);
        return this;
    }

    public DagBuilder tables(Table[] tables) {
        this.dag.setTables(tables);
        return this;
    }

    public Dag build() {
        //this.target.validate("");
        return this.dag;
    }
}
