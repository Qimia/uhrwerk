package io.qimia.uhrwerk.config.representation;


public class Tables {

    private String area;
    private String vertical;
    private String table;
    private String version;
    private Integer parallelism;
    private Integer max_bulk_size;
    private Partition[] partition;
    private Sources[] sources;
    private Targets[] targets;
    private Dependencies[] dependencies;


    public Tables() {}


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

    public Partition[] getPartition() {
        return partition;
    }

    public void setPartition(Partition[] partition) {
        this.partition = partition;
    }

    public Sources[] getSources() {
        return sources;
    }

    public void setSources(Sources[] sources) {
        this.sources = sources;
    }

    public Targets[] getTargets() {
        return targets;
    }

    public void setTargets(Targets[] targets) {
        this.targets = targets;
    }

    public Dependencies[] getDependencies() {
        return dependencies;
    }

    public void setDependencies(Dependencies[] dependencies) {
        this.dependencies = dependencies;
    }
}
