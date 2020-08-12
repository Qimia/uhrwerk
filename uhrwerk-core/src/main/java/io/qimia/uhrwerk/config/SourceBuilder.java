package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Partition;
import io.qimia.uhrwerk.config.representation.Source;

public class SourceBuilder {
    private String connectionName;
    private String path;
    private String format;
    private String version;
    private Partition partition;

    public SourceBuilder withConnectionName(String connectionName) {
        this.connectionName = connectionName;
        return this;
    }

    public SourceBuilder withPath(String path) {
        this.path = path;
        return this;
    }

    public SourceBuilder withFormat(String format) {
        this.format = format;
        return this;
    }

    public SourceBuilder withVersion(String version) {
        this.version = version;
        return this;
    }

    public SourceBuilder withPartition(Partition partition) {
        this.partition = partition;
        return this;
    }

    public Source build(){
        return new Source();
    }
}
