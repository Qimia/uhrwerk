package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.File;
import io.qimia.uhrwerk.config.representation.JDBC;
import io.qimia.uhrwerk.config.representation.S3;

import java.util.ArrayList;

public class OldConnectionBuilder {
  private ArrayList<Connection> connectionsList;
  private Connection[] connections;

  public OldConnectionBuilder() {
    this.connectionsList = new ArrayList<>();

  }

  public OldConnectionBuilder name(String name) {
    Connection connection = new Connection();
    this.connectionsList.add(connection);
    this.connectionsList.get(this.connectionsList.size() - 1).setName(name);
    return this;
  }

  public OldConnectionBuilder jdbc(JDBC jdbc) {
    if (this.connectionsList.size() != 0) {
      this.connectionsList.get(this.connectionsList.size() - 1).setJdbc(jdbc);
    }
    return this;
  }

  public OldConnectionBuilder jdbc() {
    if (this.connectionsList.size() != 0) {
      this.connectionsList.get(this.connectionsList.size() - 1).setJdbc(new JDBC());
    }
    return this;
  }

  public OldConnectionBuilder jdbcUrl(String url) {
    if (this.connectionsList.size() != 0) {
      if (this.connectionsList.get(this.connectionsList.size() - 1).getJdbc() != null) {
        this.connectionsList.get(this.connectionsList.size() - 1).getJdbc().setJdbc_url(url);
      }
    }
    return this;
  }

  public OldConnectionBuilder jdbcDriver(String driver) {
    if (this.connectionsList.size() != 0) {
      if (this.connectionsList.get(this.connectionsList.size() - 1).getJdbc() != null) {
        this.connectionsList.get(this.connectionsList.size() - 1).getJdbc().setJdbc_driver(driver);
      }
    }
    return this;
  }

  public OldConnectionBuilder user(String user) {
    if (this.connectionsList.size() != 0) {
      if (this.connectionsList.get(this.connectionsList.size() - 1).getJdbc() != null) {
        this.connectionsList.get(this.connectionsList.size() - 1).getJdbc().setUser(user);
      }
    }
    return this;
  }

  public OldConnectionBuilder pass(String pass) {
    if (this.connectionsList.size() != 0) {
      if (this.connectionsList.get(this.connectionsList.size() - 1).getJdbc() != null) {
        this.connectionsList.get(this.connectionsList.size() - 1).getJdbc().setPass(pass);
      }
    }
    return this;
  }

  public OldConnectionBuilder s3(S3 s3) {
    if (this.connectionsList.size() != 0) {
      this.connectionsList.get(this.connectionsList.size() - 1).setS3(s3);
    }
    return this;
  }

  public OldConnectionBuilder s3() {
    if (this.connectionsList.size() != 0) {
      this.connectionsList.get(this.connectionsList.size() - 1).setS3(new S3());
    }
    return this;
  }

  public OldConnectionBuilder path(String path) {
    if (this.connectionsList.size() != 0) {
      if (this.connectionsList.get(this.connectionsList.size() - 1).getS3() != null) {
        if (this.connectionsList.get(this.connectionsList.size() - 1).getS3().getPath() == null) {
          this.connectionsList.get(this.connectionsList.size() - 1).getS3().setPath(path);
        }
      }
      if (this.connectionsList.get(this.connectionsList.size() - 1).getFile() != null) {
        if (this.connectionsList.get(this.connectionsList.size() - 1).getFile().getPath() == null) {
          this.connectionsList.get(this.connectionsList.size() - 1).getFile().setPath(path);
        }
      }
    }
    return this;
  }

  public OldConnectionBuilder secretId(String secretId) {
    if (this.connectionsList.size() != 0) {
      if (this.connectionsList.get(this.connectionsList.size() - 1).getS3() != null) {
        this.connectionsList.get(this.connectionsList.size() - 1).getS3().setSecret_id(secretId);
      }
    }
    return this;
  }

  public OldConnectionBuilder secretKey(String secretKey) {
    if (this.connectionsList.size() != 0) {
      if (this.connectionsList.get(this.connectionsList.size() - 1).getS3() != null) {
        this.connectionsList.get(this.connectionsList.size() - 1).getS3().setSecret_key(secretKey);
      }
    }
    return this;
  }

  public OldConnectionBuilder file(File file) {
    if (this.connectionsList.size() != 0) {
      this.connectionsList.get(this.connectionsList.size() - 1).setFile(file);
    }
    return this;
  }

  public OldConnectionBuilder file() {
    if (this.connectionsList.size() != 0) {
      this.connectionsList.get(this.connectionsList.size() - 1).setFile(new File());
    }
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
