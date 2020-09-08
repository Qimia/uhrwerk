package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

import java.util.Objects;

public class Env {
  Metastore metastore;

  public Metastore getMetastore() {
    return metastore;
  }

  public void setMetastore(Metastore metastore) {
    this.metastore = metastore;
  }

  public void validate(String path){
    path += "env/";
    if(metastore == null){
      throw new ConfigException("Missing field: " + path + "metastore");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Env env = (Env) o;
    return Objects.equals(metastore, env.metastore);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metastore);
  }

  @Override
  public String toString() {
    return "Env{" + "metastore=" + metastore + '}';
  }
}
