package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Dependency;
import io.qimia.uhrwerk.config.representation.Transform;

public class DependencyBuilder {
    private String connectionName;
    private String area;
    private String vertical;
    private String table;
    private String format;
    private String version;
    private Transform transform;

    public DependencyBuilder withConnectionName(String connectionName) {
        this.connectionName = connectionName;
        return this;
    }

    public DependencyBuilder withArea(String area) {
        this.area = area;
        return this;
    }

    public DependencyBuilder withVertical(String vertical) {
        this.vertical = vertical;
        return this;
    }

    public DependencyBuilder withTable(String table) {
        this.table = table;
        return this;
    }

    public DependencyBuilder withFormat(String format) {
        this.format = format;
        return this;
    }

    public DependencyBuilder withVersion(String version) {
        this.version = version;
        return this;
    }

    public DependencyBuilder withTransform(Transform transform) {
        this.transform = transform;
        return this;
    }

    public Dependency build(){
        Dependency dependency = new Dependency();
        dependency.setArea(this.area);
        dependency.setConnection_name(this.connectionName);
        dependency.setFormat(this.format);
        dependency.setTable(this.table);
        dependency.setTransform(this.transform);
        dependency.setVersion(this.version);
        dependency.setVertical(this.vertical);
        return dependency;
    }
}
