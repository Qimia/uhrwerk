package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

import java.util.Arrays;

public class Partition {

  private String unit;
  private Integer size;

  public Partition() {}

  public String getUnit() {
    return unit;
  }

  public void setUnit(String unit) {
    this.unit = unit;
  }

  public Integer getSize() {
    return size;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  public void validate(String path) {
    path += "partition/";
    if (unit == null) {
      throw new ConfigException("Missing field: " + path + "unit");
    }
    if (!Arrays.asList("weeks","days","hours","minutes").contains(unit)) {
      throw new ConfigException("Wrong unit! '" + unit + "' is not allowed in " + path + "unit");
    }
    if (size == null) {
      throw new ConfigException("Missing field: " + path + "size");
    }
  }

  public void validate(String path, String type) {
    path += "partition/";
    if (type.equals("temporal_aggregate")) {
      if (unit == null) {
        throw new ConfigException("Missing field: " + path + "unit");
      }
      if (!Arrays.asList("weeks","days","hours","minutes").contains(unit)) {
        throw new ConfigException("Wrong unit! '" + unit + "' is not allowed in " + path + "unit");
      }
    }
    if (size == 0) {
      throw new ConfigException("Missing field: " + path + "size");
    }
  }

  @Override
  public String toString() {
    return "Partition{" + "unit='" + unit + '\'' + ", size=" + size + '}';
  }
}
