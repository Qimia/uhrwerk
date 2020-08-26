package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Dependency;

public class DependencyBuilder {
  private TableBuilder parent;
  private TransformBuilder transformBuilder;
  private String area;
  private String vertical;
  private String table;
  private String format;
  private String version;

  public DependencyBuilder() {}

  public DependencyBuilder(TableBuilder parent) {
    this.parent = parent;
  }

  public DependencyBuilder area(String area) {
    this.area = area;
    return this;
  }

  public DependencyBuilder vertical(String vertical) {
    this.vertical = vertical;
    return this;
  }

  public DependencyBuilder table(String table) {
    this.table = table;
    return this;
  }

  public DependencyBuilder format(String format) {
    this.format = format;
    return this;
  }

  public DependencyBuilder version(String version) {
    this.version = version;
    return this;
  }

  public TransformBuilder transform() {
    this.transformBuilder = new TransformBuilder(this);
    return this.transformBuilder;
  }

  public TableBuilder done() {
    return this.parent;
  }

  public Dependency build() {
    var dependency = new Dependency();
    dependency.setArea(this.area);
    dependency.setVertical(this.vertical);
    dependency.setTable(this.table);
    dependency.setFormat(this.format);
    dependency.setVersion(this.version);
    dependency.setTransform(this.transformBuilder.build());
    dependency.validate("");
    return dependency;
  }
}
