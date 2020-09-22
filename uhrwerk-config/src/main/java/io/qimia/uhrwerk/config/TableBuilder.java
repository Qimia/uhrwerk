package io.qimia.uhrwerk.config;




import io.qimia.uhrwerk.config.representation.*;


import java.util.ArrayList;

public class TableBuilder {
    private DagBuilder parent;
    private SourceBuilder sourceBuilder;
    private TargetBuilder targetBuilder;
    private DependencyBuilder dependencyBuilder;
    private TablePartitionBuilder partitionBuilder;

    private String area;
    private String vertical;
    private String table;
    private String version;
    private String className;
    private int parallelism;
    private int maxBulkSize;
    private Partition partition;
    private Source[] sources;
    private Target[] targets;
    private Dependency[] dependencies;
    private ArrayList<Source> sourcesList = new ArrayList<Source>();
    private ArrayList<Dependency> dependenciesList = new ArrayList<Dependency>();
    private ArrayList<Target> targetsList = new ArrayList<Target>();


    public TableBuilder() {}

    public TableBuilder(DagBuilder parent) { this.parent = parent; }

    public TableBuilder area(String area){
        this.area = area;
        return this;
    }

    public TableBuilder vertical(String vertical){
        this.vertical = vertical;
        return this;
    }

    public TableBuilder table(String table){
        this.table = table;
        return this;
    }

    public TableBuilder version(String version){
        this.version = version;
        return this;
    }

    public TableBuilder className(String className){
        this.className = className;
        return this;
    }

    public TableBuilder parallelism(int parallelism){
        this.parallelism = parallelism;
        return this;
    }

    public TableBuilder maxBulkSize(int maxBulkSize){
        this.maxBulkSize = maxBulkSize;
        return this;
    }

    public TablePartitionBuilder partition() {
        this.partitionBuilder = new TablePartitionBuilder(this);
        return this.partitionBuilder;
    }

    public TableBuilder partition(Partition partition) {
        this.partition = partition;
        return this;
    }

    public TableBuilder partition(PartitionBuilder partitionBuilder) {
        this.partition = partitionBuilder.build();
        return this;
    }

    public SourceBuilder source() {
        this.sourceBuilder = new SourceBuilder(this);
        return this.sourceBuilder;
    }

    public TableBuilder source(Source source) {
        this.sourcesList.add(source);
        return this;
    }

    public TableBuilder sources(Source[] sources) {
        this.sources = sources;
        return this;
    }

    public TableBuilder sources(SourceBuilder[] sourceBuilders) {
        this.sources = new Source[sourceBuilders.length];
        for(int i=0; i<sourceBuilders.length;i++){
            this.sources[i] = sourceBuilders[i].build();
        }
        return this;
    }

    public DependencyBuilder dependency() {
        this.dependencyBuilder = new DependencyBuilder(this);
        return this.dependencyBuilder;
    }

    public TableBuilder dependency(Dependency dependency) {
        this.dependenciesList.add(dependency);
        return this;
    }

    public TableBuilder dependencies(Dependency[] dependencies) {
        this.dependencies = dependencies;
        return this;
    }

    public TableBuilder dependencies(DependencyBuilder[] dependencyBuilders) {
        this.dependencies = new Dependency[dependencyBuilders.length];
        for(int i=0; i<dependencyBuilders.length;i++){
            this.dependencies[i] = dependencyBuilders[i].build();
        }
        return this;
    }

    public TargetBuilder target() {
        this.targetBuilder = new TargetBuilder(this);
        return this.targetBuilder;
    }

    public TableBuilder target(Target target) {
        this.targetsList.add(target);
        return this;
    }

    public TableBuilder targets(Target[] targets) {
        this.targets = targets;
        return this;
    }

    public TableBuilder targets(TargetBuilder[] targetBuilders) {
        this.targets = new Target[targetBuilders.length];
        for(int i=0; i<targetBuilders.length;i++){
            this.targets[i] = targetBuilders[i].build();
        }
        return this;
    }

    public DagBuilder done() {
        this.parent.table(this.buildRepresentationTable());
        return this.parent;
    }

    public Table buildRepresentationTable() {
        var table = new Table();
        table.setArea(this.area);
        table.setVertical(this.vertical);
        table.setTable(this.table);
        table.setVersion(this.version);
        table.setParallelism(this.parallelism);
        table.setMax_bulk_size(this.maxBulkSize);
        table.setPartition(this.partition);
        if (this.sources != null) {
            table.setSources(this.sources);
        }
        else {
            if (this.sourcesList.size() != 0) {
                this.sources = new Source[sourcesList.size()];
                sourcesList.toArray(this.sources);
                table.setSources(this.sources);
            }
        }

        if (this.dependencies != null) {
            table.setDependencies(this.dependencies);
        }
        else {
            if (this.dependenciesList.size() != 0) {
                this.dependencies = new Dependency[dependenciesList.size()];
                dependenciesList.toArray(this.dependencies);
                table.setDependencies(this.dependencies);
            }
        }

        if (this.targets != null) {
            table.setTargets(this.targets);
        }
        else {
            if (this.targetsList.size() != 0) {
                this.targets = new Target[targetsList.size()];
                targetsList.toArray(this.targets);
                table.setTargets(this.targets);
            }
        }
        return table;
    }

    public io.qimia.uhrwerk.common.model.Table build() {
        var table = new Table();
        table.setArea(this.area);
        table.setVertical(this.vertical);
        table.setTable(this.table);
        table.setVersion(this.version);
        table.setClass_name(this.className);
        table.setParallelism(this.parallelism);
        table.setMax_bulk_size(this.maxBulkSize);
        table.setPartition(this.partition);
        if (this.sources != null) {
            table.setSources(this.sources);
        }
        else {
            if (this.sourcesList.size() != 0) {
                this.sources = new Source[sourcesList.size()];
                sourcesList.toArray(this.sources);
                table.setSources(this.sources);
            }
        }

        if (this.dependencies != null) {
            table.setDependencies(this.dependencies);
        }
        else {
            if (this.dependenciesList.size() != 0) {
                this.dependencies = new Dependency[dependenciesList.size()];
                dependenciesList.toArray(this.dependencies);
                table.setDependencies(this.dependencies);
            }
        }

        if (this.targets != null) {
            table.setTargets(this.targets);
        }
        else {
            if (this.targetsList.size() != 0) {
                this.targets = new Target[targetsList.size()];
                targetsList.toArray(this.targets);
                table.setTargets(this.targets);
            }
        }
        YamlConfigReader configReader = new YamlConfigReader();
        return configReader.getModelTable(table);
    }
}
