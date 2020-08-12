package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.*;

public class TableBuilder {
    private String area;
    private String vertical;
    private String table;
    private String version;
    private Integer parallelism;
    private Integer maxBulkSize;
    private Partition partition;
    private Source[] source;
    private Target[] targets;
    private Dependency[] dependencies;

    public TableBuilder withArea(String area) {
        this.area = area;
        return this;
    }

    public TableBuilder withVertical(String vertical) {
        this.vertical = vertical;
        return this;
    }

    public TableBuilder withTable(String table) {
        this.table = table;
        return this;
    }

    public TableBuilder withVersion(String version) {
        this.version = version;
        return this;
    }

    public TableBuilder withParallelism(Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public TableBuilder withMaxBulkSize(Integer maxBulkSize) {
        this.maxBulkSize = maxBulkSize;
        return this;
    }

    public TableBuilder withPartition(Partition partition) {
        this.partition = partition;
        return this;
    }

    public TableBuilder withSource(Source[] source) {
        this.source = source;
        return this;
    }

    public TableBuilder withTargets(Target[] targets) {
        this.targets = targets;
        return this;
    }

    public TableBuilder withDependencies(Dependency[] dependencies) {
        this.dependencies = dependencies;
        return this;
    }

    public Table build(){
        return new Table();
    }
}
