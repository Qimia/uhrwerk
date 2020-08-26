package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Source;

public class SourceBuilder {
  private TableBuilder parent;
  private SourcePartitionBuilder sourcePartitionBuilder;
  private ParallelLoadBuilder parallelLoadBuilder;
  private SelectBuilder selectBuilder;

  private String connectionName;
  private String path;
  private String format;
  private String version;

  public SourceBuilder() {}

  public SourceBuilder(TableBuilder parent) {
    this.parent = parent;
  }

  public SourceBuilder connectionName(String connectionName) {
    this.connectionName = connectionName;
    return this;
  }

  public SourceBuilder path(String path) {
    this.path = path;
    return this;
  }

  public SourceBuilder format(String format) {
    this.format = format;
    return this;
  }

  public SourceBuilder version(String version) {
    this.version = version;
    return this;
  }

  public SourcePartitionBuilder partition() {
    this.sourcePartitionBuilder = new SourcePartitionBuilder(this);
    return this.sourcePartitionBuilder;
  }

  public ParallelLoadBuilder parallelLoad() {
    this.parallelLoadBuilder = new ParallelLoadBuilder(this);
    return this.parallelLoadBuilder;
  }

  public SelectBuilder select() {
    this.selectBuilder = new SelectBuilder(this);
    return this.selectBuilder;
  }

  public TableBuilder done() {
    return this.parent;
  }

  public Source build() {
    var source = new Source();
    source.setConnection_name(this.connectionName);
    source.setPath(this.path);
    source.setFormat(this.format);
    source.setVersion(this.version);
    source.setPartition(this.sourcePartitionBuilder.build());
    source.setParallel_load(this.parallelLoadBuilder.build());
    source.setSelect(this.selectBuilder.build());
    source.validate("");
    return source;
  }
}
