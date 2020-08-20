package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.File;
import io.qimia.uhrwerk.config.representation.S3;
import io.qimia.uhrwerk.config.representation.JDBC;

import java.util.ArrayList;

public class ConnectionBuilder {
  private ArrayList<io.qimia.uhrwerk.config.representation.Connection> connectionsList;
  private io.qimia.uhrwerk.config.representation.Connection[] connections;
  private io.qimia.uhrwerk.config.representation.JDBC jdbc;
  private io.qimia.uhrwerk.config.representation.S3 s3;
  private io.qimia.uhrwerk.config.representation.File file;

  public ConnectionBuilder() {
    this.connectionsList = new ArrayList<io.qimia.uhrwerk.config.representation.Connection>();
  }

  public ConnectionBuilder name(String name) {
    Connection connection = new Connection();
    this.connectionsList.add(connection);
    this.connectionsList.get(this.connectionsList.size()-1).setName(name);
    return this;
  }

  public ConnectionBuilder jdbc(JDBC jdbc) {
    this.connectionsList.get(this.connectionsList.size()-1).setJdbc(jdbc);
    return this;
  }

  public ConnectionBuilder jdbc() {
    if (this.jdbc == null){
      this.jdbc = new JDBC();
    }
    this.connectionsList.get(this.connectionsList.size()-1).setJdbc(this.jdbc);
    return this;
  }

  public ConnectionBuilder jdbc_url(String url) {
    if (this.connectionsList != null){
      if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
        this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setJdbc_url(url);
      } else {
        System.out.println("There is no JDBC object to which one can set a jdbc_url");
      }
    }
    return this;
  }

  public ConnectionBuilder jdbc_driver(String driver) {
    if (this.connectionsList != null){
      if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
        this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setJdbc_driver(driver);
      } else {
        System.out.println("There is no JDBC object to which one can set a jdbc_driver");
      }
    }
    return this;
  }

  public ConnectionBuilder user(String user) {
    if (this.connectionsList != null){
      if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
        this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setUser(user);
      } else {
        System.out.println("There is no JDBC object to which one can set a user");
      }
    }
    return this;
  }

  public ConnectionBuilder pass(String pass) {
    if (this.connectionsList != null){
      if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
        this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setPass(pass);
      } else {
        System.out.println("There is no JDBC object to which one can set a pass");
      }
    }
    return this;
  }


  public ConnectionBuilder s3(S3 s3) {
    this.connectionsList.get(this.connectionsList.size()-1).setS3(s3);
    return this;
  }

  public ConnectionBuilder s3() {
    if (this.s3 == null){
      this.s3 = new S3();
    }
    this.connectionsList.get(this.connectionsList.size()-1).setS3(this.s3);
    return this;
  }


  public ConnectionBuilder path(String path) {
    if (this.connectionsList != null){
      if (this.connectionsList.get(this.connectionsList.size()-1).getS3() == null
      && this.connectionsList.get(this.connectionsList.size()-1).getFile() == null){
        System.out.println("There is no S3 or File object to which one can set a path");
      }
      if (this.connectionsList.get(this.connectionsList.size()-1).getS3() != null) {
        if (this.connectionsList.get(this.connectionsList.size() - 1).getS3().getPath() == null) {
          this.connectionsList.get(this.connectionsList.size() - 1).getS3().setPath(path);
        }
      }
      if (this.connectionsList.get(this.connectionsList.size()-1).getFile() != null) {
        if (this.connectionsList.get(this.connectionsList.size() - 1).getFile().getPath() == null) {
          this.connectionsList.get(this.connectionsList.size() - 1).getFile().setPath(path);
        }
      }
    }
    return this;
  }



  public ConnectionBuilder secret_id(String secret_id) {
    if (this.connectionsList != null){
      if (this.connectionsList.get(this.connectionsList.size()-1).getS3() != null){
        this.connectionsList.get(this.connectionsList.size()-1).getS3().setSecret_id(secret_id);
      } else {
        System.out.println("There is no S3 object to which one can set a secret_id");
      }
    }
    return this;
  }

  public ConnectionBuilder secret_key(String secret_key) {
    if (this.connectionsList != null){
      if (this.connectionsList.get(this.connectionsList.size()-1).getS3() != null){
        this.connectionsList.get(this.connectionsList.size()-1).getS3().setSecret_key(secret_key);
      } else {
        System.out.println("There is no S3 object to which one can set a secret_key");
      }
    }
    return this;
  }


  public ConnectionBuilder file(File file) {
    this.connectionsList.get(this.connectionsList.size()-1).setFile(file);
    return this;
  }

  public ConnectionBuilder file() {
    if (this.file == null){
      this.file = new File();
    }
    this.connectionsList.get(this.connectionsList.size()-1).setFile(this.file);
    return this;
  }


  public io.qimia.uhrwerk.common.model.Connection[] build() {
    if (this.connectionsList != null) {
      this.connections = new Connection[connectionsList.size()];
      connectionsList.toArray(this.connections);
    }
    YamlConfigReader configReader = new YamlConfigReader();
    return configReader.getModelConnections(this.connections);
  }
}
