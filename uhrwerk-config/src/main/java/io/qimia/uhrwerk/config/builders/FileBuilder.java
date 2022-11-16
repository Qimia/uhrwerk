package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.config.representation.File;

public class FileBuilder {
  private String path;
  private ConnectionBuilder parent;

  public FileBuilder() {}

  public FileBuilder(ConnectionBuilder parent) {
    this.parent = parent;
  }

  public FileBuilder path(String path) {
    this.path = path;
    return this;
  }


  public ConnectionBuilder done() {
    this.parent.file(this.build());
    return this.parent;
  }

  public File build() {
    var file = new File();
    file.setPath(this.path);
    file.validate("");
    return file;
  }
}
