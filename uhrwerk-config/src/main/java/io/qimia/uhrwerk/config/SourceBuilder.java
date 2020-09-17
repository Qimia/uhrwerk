package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.ParallelLoad;
import io.qimia.uhrwerk.config.representation.Partition;
import io.qimia.uhrwerk.config.representation.Select;
import io.qimia.uhrwerk.config.representation.Source;

public class SourceBuilder {
  private TableBuilder parent;
  private SourcePartitionBuilder partitionBuilder;
  private ParallelLoadBuilder parallelLoadBuilder;
  private SelectBuilder selectBuilder;

  private String connectionName;
  private String path;
  private String format;
  private String version;
  private Partition partition;
  private ParallelLoad parallelLoad;
  private Select select;
  private Boolean autoloading = true;

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
    this.partitionBuilder = new SourcePartitionBuilder(this);
    return this.partitionBuilder;
  }

  public SourceBuilder partition(Partition partition) {
    this.partition = partition;
    return this;
  }

  public SourceBuilder partition(PartitionBuilder partitionBuilder) {
    this.partition = partitionBuilder.build();
    return this;
  }

  public ParallelLoadBuilder parallelLoad() {
    this.parallelLoadBuilder = new ParallelLoadBuilder(this);
    return this.parallelLoadBuilder;
  }

  public SourceBuilder parallelLoad(ParallelLoad parallelLoad) {
    this.parallelLoad = parallelLoad;
    return this;
  }

  public SourceBuilder parallelLoad(ParallelLoadBuilder parallelLoadBuilder) {
    this.parallelLoad = parallelLoadBuilder.build();
    return this;
  }

  public SelectBuilder select() {
    this.selectBuilder = new SelectBuilder(this);
    return this.selectBuilder;
  }

  public SourceBuilder select(Select select) {
    this.select = select;
    return this;
  }

  public SourceBuilder select(SelectBuilder selectBuilder) {
    this.select = selectBuilder.build();
    return this;
  }

  public SourceBuilder autoloading(Boolean autoloading){
    this.autoloading = autoloading;
    return this;
  }

  public TableBuilder done() {
    this.parent.source(this.build());
    return this.parent;
  }

  public Source build() {
    var source = new Source();
    source.setConnection_name(this.connectionName);
    source.setPath(this.path);
    source.setFormat(this.format);
    source.setVersion(this.version);
    source.setPartition(this.partition);
    source.setParallel_load(this.parallelLoad);
    source.setSelect(this.select);
    source.setAutoloading(this.autoloading);
    source.validate("");
    return source;
  }
}
