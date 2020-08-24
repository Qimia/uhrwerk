package io.qimia.uhrwerk.config.representation;


import io.qimia.uhrwerk.config.ConfigException;

public class Table{

    private String area;
    private String vertical;
    private String table;
    private String version;
    private Integer parallelism=1;
    private Integer max_bulk_size=1;
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

    public boolean sourcesSet() {
        return sources != null;
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

    public void validate(String path){
        path += "table/";
        if(area == null){
            throw new ConfigException("Missing field: " + path + "area");
        }
        if(vertical == null){
            throw new ConfigException("Missing field: " + path + "vertical");
        }
        if(table == null){
            throw new ConfigException("Missing field: " + path + "table");
        }
        if(version == null){
            throw new ConfigException("Missing field: " + path + "version");
        }
        if(parallelism == null){
            throw new ConfigException("Missing field: " + path + "parallelism");
        }
        if(max_bulk_size == null){
            throw new ConfigException("Missing field: " + path + "max_bulk_size");
        }
        if(partition == null){
            throw new ConfigException("Missing field: " + path + "partition");
        }
        else{
            partition.validate(path);
        }
        if(sources==null && dependencies==null){
            throw new ConfigException("Missing field: " + path + "sources or dependencies");
        }
        else {
            if(dependencies!=null){
                for(Dependency d: dependencies){
                    d.validate(path);
                }
            }
            if(sources!=null){
                for(Source s: sources){
                    s.validate(path);
                }
            }
        }
        if(targets==null){
            throw new ConfigException("Missing field: " + path + "targets");
        }
        else{
            for(Target t: targets){
                t.validate(path);
            }
        }
    }
}
