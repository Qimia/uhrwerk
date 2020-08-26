package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.ParallelLoad;

public class ParallelLoadBuilder {
  private SourceBuilder parent;
  private String query;
  private String column;
  private Integer num;

  public ParallelLoadBuilder() {}

  public ParallelLoadBuilder(SourceBuilder parent) {
    this.parent = parent;
  }

  public ParallelLoadBuilder query(String query) {
    this.query = query;
    return this;
  }

  public ParallelLoadBuilder column(String column) {
    this.column = column;
    return this;
  }

  public ParallelLoadBuilder num(Integer num) {
    this.num = num;
    return this;
  }

  public SourceBuilder done() {
    return this.parent;
  }

  public ParallelLoad build() {
    ParallelLoad parallelLoad = new ParallelLoad();
    parallelLoad.setColumn(this.column);
    parallelLoad.setNum(this.num);
    parallelLoad.setQuery(this.query);
    return parallelLoad;
  }
}
