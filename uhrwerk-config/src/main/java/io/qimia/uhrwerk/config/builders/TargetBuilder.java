package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.config.representation.Target;

public class TargetBuilder {
  private String connectionName;
  private String format;
  private TableBuilder parent;

  public TargetBuilder() {}

  public TargetBuilder(TableBuilder parent) {
    this.parent = parent;
  }

  public TargetBuilder connectionName(String connectionName) {
    this.connectionName = connectionName;
    return this;
  }

  public TargetBuilder format(String format) {
    this.format = format;
    return this;
  }

  public TableBuilder done() {
    this.parent.target(this.build());
    return this.parent;
  }

  public Target build() {
    var target = new Target();
    target.setConnectionName(this.connectionName);
    target.setFormat(this.format);
    target.validate("");
    return target;
  }
}
