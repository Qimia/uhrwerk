package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;

public class TableBuilder {
    private Table table;

    public TableBuilder(Table table) {
        this.table = table;
    }

    public TableBuilder area(String area){
        this.table.setArea(area);
        return this;
    }

    public TableBuilder vertical(String vertical){
        this.table.setVertical(vertical);
        return this;
    }

    public TableBuilder name(String name){
        this.table.setName(name);
        return this;
    }

    public TableBuilder version(String version){
        this.table.setVersion(version);
        return this;
    }

    public TableBuilder parallelism(int parallelism){
        this.table.setParallelism(parallelism);
        return this;
    }

    public TableBuilder maxBulkSize(int maxBulkSize){
        this.table.setMaxBulkSize(maxBulkSize);
        return this;
    }

    public TableBuilder partitionUnit(PartitionUnit partitionUnit){
        this.table.setPartitionUnit(partitionUnit);
        return this;
    }

    public TableBuilder partitionSize(int partitionSize){
        this.table.setPartitionSize(partitionSize);
        return this;
    }

    public TableBuilder dependencies(Dependency[] dependencies){
        this.table.setDependencies(dependencies);
        return this;
    }

    public TableBuilder targets(Target[] targets){
        this.table.setTargets(targets);
        return this;
    }

    public TableBuilder sources(Source[] sources){
        this.table.setSources(sources);
        return this;
    }

    public Table build(){
        return this.table;
    }
}
