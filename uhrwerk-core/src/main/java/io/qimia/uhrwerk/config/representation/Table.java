package io.qimia.uhrwerk.config.representation;


public class Table extends Representation{

    private String area;
    private String vertical;
    private String table;
    private String version;
    private Integer parallelism;
    private Integer max_bulk_size;
    private Partition partition;
    private Source[] sources;
    private Target[] targets;
    private Dependency[] dependencies;


    public Table() {}


    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getVertical() {
        return vertical;
    }

    public void setVertical(String vertical) {
        this.vertical = vertical;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    public Integer getMax_bulk_size() {
        return max_bulk_size;
    }

    public void setMax_bulk_size(Integer max_bulk_size) {
        this.max_bulk_size = max_bulk_size;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public Source[] getSources() {
        return sources;
    }

    public void setSources(Source[] sources) {
        this.sources = sources;
    }

    public Target[] getTargets() {
        return targets;
    }

    public void setTargets(Target[] targets) {
        this.targets = targets;
    }

    public Dependency[] getDependencies() {
        return dependencies;
    }

    public void setDependencies(Dependency[] dependencies) {
        this.dependencies = dependencies;
    }

}
