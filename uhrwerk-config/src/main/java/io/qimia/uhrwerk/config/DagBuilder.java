package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.Dag;
import io.qimia.uhrwerk.config.representation.Table;

import java.util.ArrayList;

public class DagBuilder {
    private ConnectionBuilder connectionBuilder;
    private TableBuilder tableBuilder;

    private Connection[] connections;
    private Table[] tables;
    private final ArrayList<Connection> connectionsList = new ArrayList<>();
    private final ArrayList<Table> tablesList = new ArrayList<>();

    public DagBuilder() {
    }

    public TableBuilder table() {
        this.tableBuilder = new TableBuilder(this);
        return this.tableBuilder;
    }

    public DagBuilder table(Table table) {
        this.tablesList.add(table);
        return this;
    }

    public DagBuilder tables(Table[] tables) {
        this.tables = tables;
        return this;
    }

    public DagBuilder tables(TableBuilder[] tableBuilders) {
        this.tables = new Table[tableBuilders.length];
        for(int i=0; i<tableBuilders.length;i++){
            this.tables[i] = tableBuilders[i].buildRepresentationTable();
        }
        return this;
    }


    public ConnectionBuilder connection() {
        this.connectionBuilder = new ConnectionBuilder(this);
        return this.connectionBuilder;
    }

    public DagBuilder connection(Connection connection) {
        this.connectionsList.add(connection);
        return this;
    }

    public DagBuilder connections(Connection[] connections) {
        this.connections = connections;
        return this;
    }

    public DagBuilder connections(ConnectionBuilder[] connectionBuilders) {
        this.connections = new Connection[connectionBuilders.length];
        for(int i=0; i<connectionBuilders.length;i++){
            this.connections[i] = connectionBuilders[i].buildRepresentationConnection();
        }
        return this;
    }

    public io.qimia.uhrwerk.common.model.Dag build() {
        var dag = new Dag();
        if (this.connections != null) {
            dag.setConnections(this.connections);
        }
        else {
            if (this.connectionsList.size() != 0) {
                this.connections = new Connection[connectionsList.size()];
                connectionsList.toArray(this.connections);
                dag.setConnections(this.connections);
            }
        }

        if (this.tables != null) {
            dag.setTables(this.tables);
        }
        else {
            if (this.tablesList.size() != 0) {
                this.tables = new Table[tablesList.size()];
                tablesList.toArray(this.tables);
                dag.setTables(this.tables);
            }
        }
        YamlConfigReader configReader = new YamlConfigReader();
        return configReader.getModelDag(dag);
    }
}
