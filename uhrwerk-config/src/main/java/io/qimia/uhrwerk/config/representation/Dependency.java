package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

import java.util.Arrays;

public class Dependency {

  private String area;
  private String vertical;
  private String table;
  private String format;
  private String version;
  private Transform transform;

  public Dependency() {}

  public String getArea() {
    return area;
  }

  public void setArea(String area) {
    this.area = area;
  }

  public String getVertical() {
    return vertical;
  }

  public void setVertical(String vertical) {
    this.vertical = vertical;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public Transform getTransform() {
    return transform;
  }

  public void setTransform(Transform transform) {
    this.transform = transform;
  }

  public void validate(String path) {
    path += "dependency/";
    if (area == null) {
      throw new ConfigException("Missing field: " + path + "area");
    }
    if (vertical == null) {
      throw new ConfigException("Missing field: " + path + "vertical");
    }
    if (table == null) {
      throw new ConfigException("Missing field: " + path + "table");
    }
    if (format == null) {
      throw new ConfigException("Missing field: " + path + "format");
    }
    if (!Arrays.asList("json", "parquet", "jdbc", "orc", "libsvm", "csv", "text" , "avro").contains(format)) {
      throw new ConfigException("Wrong format! '" + format + "' is not allowed in " + path + "format");
    }
    if (version == null) {
      throw new ConfigException("Missing field: " + path + "version");
    }
    if (transform != null) {
      transform.validate(path);
    }
  }

  @Override
  public String toString() {
    return "Dependency{"
        + "area='"
        + area
        + '\''
        + ", vertical='"
        + vertical
        + '\''
        + ", table='"
        + table
        + '\''
        + ", format='"
        + format
        + '\''
        + ", version='"
        + version
        + '\''
        + ", transform="
        + transform
        + '}';
  }
}
