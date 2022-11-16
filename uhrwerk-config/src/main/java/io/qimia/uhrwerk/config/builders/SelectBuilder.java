package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.config.representation.Select;

public class SelectBuilder {
  private SourceBuilder parent;
  private String query;
  private String column;

  public SelectBuilder() {}

  public SelectBuilder(SourceBuilder parent) {
    this.parent = parent;
  }

  public SelectBuilder query(String query) {
    this.query = query;
    return this;
  }

  public SelectBuilder column(String column) {
    this.column = column;
    return this;
  }

  public SourceBuilder done() {
    this.parent.select(this.build());
    return this.parent;
  }

  public Select build() {
    Select select = new Select();
    select.setColumn(this.column);
    select.setQuery(this.query);
    select.validate("");
    return select;
  }
}
