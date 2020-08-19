package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;

import io.qimia.uhrwerk.config.representation.Partition;
import io.qimia.uhrwerk.config.representation.Source;

public class TableBuilder {
    private io.qimia.uhrwerk.config.representation.Table table;
    private io.qimia.uhrwerk.config.representation.Partition partition;
    private io.qimia.uhrwerk.config.representation.Source source;

    public TableBuilder(Table table) {
        this.table = new io.qimia.uhrwerk.config.representation.Table();
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
        this.table.setTable(name);
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
        this.table.setMax_bulk_size(maxBulkSize);
        return this;
    }

    public TableBuilder partition(Partition partition){
        this.table.setPartition(partition);
        return this;
    }

    public TableBuilder partition_unit(String partition_unit) {
        if (this.partition == null) {
            this.partition = new Partition();
        }
        this.partition.setUnit(partition_unit);
        return this;
    }

    public TableBuilder partition_size(int partition_size) {
        if (this.partition == null) {
            this.partition = new Partition();
        }
        this.partition.setSize(partition_size);
        return this;
    }

    public TableBuilder source(Source[] sources){
        this.table.setSources(sources);
        return this;
    }



    public Table build(){
        return new Table();
    }
}
