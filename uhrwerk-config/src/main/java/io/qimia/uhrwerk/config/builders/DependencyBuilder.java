package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.config.representation.Dependency;
import io.qimia.uhrwerk.config.representation.Reference;

public class DependencyBuilder {
  private TableBuilder parent;
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

  public TableBuilder done() {
    this.parent.dependency(this.build());
    return this.parent;
  }

  public Dependency build() {
    var dependency = new Dependency();
    Reference reference = new Reference();
    reference.setArea(this.area);
    reference.setVertical(this.vertical);
    reference.setTable(this.table);
    reference.setVersion(this.version);
    dependency.setReference(reference);
    dependency.setFormat(this.format);
    dependency.validate("");
    return dependency;
  }
}
