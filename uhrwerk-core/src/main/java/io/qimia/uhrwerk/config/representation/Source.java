package io.qimia.uhrwerk.config.representation;


public class Source {

    private String connection_name;
    private String path;
    private String format;
    private String version;
    private Partition partition;
    private ParallelLoad parallel_load;
    private Select select;

    public Source() {}


    public String getConnection_name() {
        return connection_name;
    }

    public void setConnection_name(String connection_name) {
        this.connection_name = connection_name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public ParallelLoad getParallel_load() {
        return parallel_load;
    }

    public void setParallel_load(ParallelLoad parallel_load) {
        this.parallel_load = parallel_load;
    }

    public Select getSelect() {
        return select;
    }

    public void setSelect(Select select) {
        this.select = select;
    }
}
